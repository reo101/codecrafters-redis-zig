const Resp = @import("./Resp.zig");

const std = @import("std");
const net = std.net;
const io = std.io;

const log = std.log.scoped(.main);

pub fn main() !void {
    log.info("Program started", .{});

    const port: u16 = 6379;

    const address: net.Address = .initIp4(.{ 127, 0, 0, 1 }, port);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    log.info("Listening at port {d}", .{port});

    var state: Resp.State = .{
        // TODO: cross-thread?
        .kv = .empty,
        .allocator = std.heap.page_allocator,
    };
    defer state.kv.deinit(state.allocator);

    while (true) {
        const connection = try listener.accept();
        log.info("Accepted new connection", .{});

        var thread = try std.Thread.spawn(.{}, Resp.connectionWorker, .{connection, &state});

        thread.detach();
    }
}
