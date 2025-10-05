const std = @import("std");
const net = std.net;

const xev = @import("xev");

const Resp = @import("./Resp.zig");

const log = std.log.scoped(.server);

pub const Server = struct {
    loop: *xev.Loop,
    alloc: std.mem.Allocator,
    next_id: u64,

    fn onAccept(self_opt: ?*Server, l: *xev.Loop, _: *xev.Completion, r: xev.TCP.AcceptError!xev.TCP) xev.CallbackAction {
        const self = self_opt.?;
        var sock = r catch |err| {
            log.err("accept error: {any}", .{err});
            return .rearm;
        };

        var conn = self.alloc.create(Connection) catch {
            var c: xev.Completion = undefined;
            sock.close(l, &c, void, null, (struct {
                fn closeCb(_: ?*void, _: *xev.Loop, _: *xev.Completion, _: xev.TCP, _: xev.CloseError!void) xev.CallbackAction {
                    return .disarm;
                }
            }).closeCb);
            return .rearm;
        };
        conn.* = .{
            .id = self.next_id,
            .loop = l,
            .sock = sock,
            .alloc = self.alloc,
        };
        self.next_id += 1;
        conn.start();
        return .rearm;
    }
};

const oom_err = "-ERR OOM\r\n";
const unk_err = "-ERR unknown command\r\n";

pub const Connection = struct {
    loop: *xev.Loop,
    sock: xev.TCP,
    alloc: std.mem.Allocator,

    c_io: xev.Completion = undefined,

    // rolling parser + reply buffer
    parser: Resp.Parser = .{},
    reply_buf: [1024]u8 = undefined,

    state: *Resp.State, // KV

    // temp read chunk for xev
    read_chunk: [4096]u8 = undefined,

    pub fn start(self: *Connection) void {
        self.sock.read(self.loop, &self.c_io, .{ .slice = &self.read_chunk }, Connection, self, onRead);
    }

    fn onRead(self_opt: ?*Connection, l: *xev.Loop, c: *xev.Completion, s: xev.TCP, rb: xev.ReadBuffer, r: xev.ReadError!usize) xev.CallbackAction {
        const self = self_opt.?;
        const n = r catch |err| {
            if (err != error.EOF) {
                log.info("Read error: {any}", .{err});
            }
            s.shutdown(l, c, Connection, self, onShutdown);
            return .disarm;
        };
        if (n == 0) {
            s.shutdown(l, c, Connection, self, onShutdown);
            return .disarm;
        }

        const bytes = rb.slice[0..n];

        // NOTE: Append into rolling buffer
        if (self.parser.in_used + bytes.len > self.parser.in_buf.len) {
            log.err("input buffer overflow, closing", .{});
            s.shutdown(l, c, Connection, self, onShutdown);
            return .disarm;
        }
        std.mem.copyForwards(
            u8,
            self.parser.in_buf[self.parser.in_used .. self.parser.in_used + bytes.len],
            bytes,
        );
        self.parser.in_used += bytes.len;

        // NOTE: Try to parse one full command
        const consumed = Resp.tryParseOneInPlace(&self.parser) catch |e| switch (e) {
            Resp.Err.NeedMore => return .rearm, // keep reading until we hit CRLF/body
            else => {
                const err = "-ERR protocol error\r\n";
                s.write(l, c, .{ .slice = err }, Connection, self, onWrote);
                self.parser.in_used = 0;
                return .disarm;
            },
        };

        const argv = self.parser.argv[0..self.parser.argc];

        log.info("Parsed a command: {f}", .{Resp.FmtArgv{ .argv = argv }});

        // PING
        if (std.mem.eql(u8, argv[0], "PING")) {
            const reply = "+PONG\r\n";
            s.write(l, c, .{ .slice = reply }, Connection, self, onWrote);
            Resp.compact(&self.parser, consumed);
            return .disarm;
        }
        // ECH
        if (std.mem.eql(u8, argv[0], "ECHO") and argv.len == 2) {
            const reply = Resp.writeBulkInto(&self.reply_buf, argv[1]);
            s.write(l, c, .{ .slice = reply }, Connection, self, onWrote);
            Resp.compact(&self.parser, consumed);
            return .disarm;
        }
        // SET key val (store copies so they survive buffer compaction)
        if (std.mem.eql(u8, argv[0], "SET") and argv.len == 3) {
            const key_copy = self.state.allocator.dupe(u8, argv[1]) catch {
                s.write(l, c, .{ .slice = oom_err }, Connection, self, onWrote);
                Resp.compact(&self.parser, consumed);
                return .disarm;
            };
            const val_copy = self.state.allocator.dupe(u8, argv[2]) catch {
                self.state.allocator.free(key_copy);
                s.write(l, c, .{ .slice = oom_err }, Connection, self, onWrote);
                Resp.compact(&self.parser, consumed);
                return .disarm;
            };
            _ = self.state.kv.put(self.state.allocator, key_copy, val_copy) catch {
                self.state.allocator.free(key_copy);
                self.state.allocator.free(val_copy);
                s.write(l, c, .{ .slice = oom_err }, Connection, self, onWrote);
                Resp.compact(&self.parser, consumed);
                return .disarm;
            };
            const ok = Resp.writeBulkInto(&self.reply_buf, "OK");
            s.write(l, c, .{ .slice = ok }, Connection, self, onWrote);
            Resp.compact(&self.parser, consumed);
            return .disarm;
        }
        // GET key
        if (std.mem.eql(u8, argv[0], "GET") and argv.len == 2) {
            const k = argv[1];
            const v = self.state.kv.get(k);
            const reply = Resp.writeBulkInto(&self.reply_buf, v);
            s.write(l, c, .{ .slice = reply }, Connection, self, onWrote);
            Resp.compact(&self.parser, consumed);
            return .disarm;
        }

        // Unknown or wrong arity
        s.write(l, c, .{ .slice = unk_err }, Connection, self, onWrote);
        Resp.compact(&self.parser, consumed);
        return .disarm;
    }

    fn onWrote(self_opt: ?*Connection, l: *xev.Loop, c: *xev.Completion, s: xev.TCP, _: xev.WriteBuffer, r: xev.WriteError!usize) xev.CallbackAction {
        _ = r catch {
            s.shutdown(l, c, Connection, self_opt.?, onShutdown);
            return .disarm;
        };
        // After writing, go back to reading using the same completion
        const self = self_opt.?;
        s.read(l, c, .{ .slice = &self.reply_buf }, Connection, self, onRead);
        return .disarm;
    }

    fn onShutdown(self_opt: ?*Connection, l: *xev.Loop, c: *xev.Completion, s: xev.TCP, r: xev.ShutdownError!void) xev.CallbackAction {
        _ = r catch {};
        s.close(l, c, Connection, self_opt.?, onClosed);
        return .disarm;
    }

    fn onClosed(self_opt: ?*Connection, _: *xev.Loop, _: *xev.Completion, _: xev.TCP, _: xev.CloseError!void) xev.CallbackAction {
        var self = self_opt.?;
        self.alloc.destroy(self);
        return .disarm;
    }
};

pub const Ctx = struct {
    loop: *xev.Loop,
    allocator: std.mem.Allocator,
    state: *Resp.State,
};

pub fn acceptCb(ctx_opt: ?*Ctx, l: *xev.Loop, _: *xev.Completion, r: xev.AcceptError!xev.TCP) xev.CallbackAction {
    const ctx = ctx_opt.?;
    var sock = r catch |err| {
        log.err("Accept error: {any}", .{err});
        return .rearm;
    };

    var conn = ctx.allocator.create(Connection) catch {
        var c: xev.Completion = undefined;
        sock.close(l, &c, void, null, struct {
            fn cb(_: ?*void, _: *xev.Loop, _: *xev.Completion, _: xev.TCP, _: xev.CloseError!void) xev.CallbackAction {
                return .disarm;
            }
        }.cb);
        return .rearm;
    };
    conn.* = .{
        .loop = l,
        .sock = sock,
        .alloc = ctx.allocator,
        .state = ctx.state,
    };
    conn.start();

    log.info("Accepted connection", .{});

    return .rearm;
}
