const Server = @import("./Server.zig");
const Resp = @import("./Resp.zig");

const std = @import("std");
const net = std.net;
const io = std.io;
const posix = std.posix;

const xev = @import("xev");

const log = std.log.scoped(.main);

pub fn main() !void {
    log.info("Program started", .{});

    var tpool = xev.ThreadPool.init(.{});
    defer tpool.deinit();
    defer tpool.shutdown();
    var loop = try xev.Loop.init(.{ .thread_pool = &tpool });
    defer loop.deinit();

    const port: u16 = 6379;

    const address: net.Address = .initIp4(.{ 127, 0, 0, 1 }, port);

    var server: xev.TCP = try .init(address);
    // NOTE: set `REUSEPORT` (needed for running consecutive tests)
    try posix.setsockopt(server.fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));

    try server.bind(address);
    try server.listen(1);

    log.info("Listening at port {d}", .{port});

    var c_accept: xev.Completion = undefined;

    var state: Resp.State = .{
        .kv = .empty,
        .allocator = std.heap.c_allocator,
    };
    defer state.kv.deinit(state.allocator);

    var ctx = Server.Ctx{
        .loop = &loop,
        // NOTE: per-connection allocator
        // TODO: replace with an arena
        .allocator = std.heap.c_allocator,
        .state = &state,
    };

    // NOTE: `accept` rearms itself after accepting a connection
    server.accept(&loop, &c_accept, Server.Ctx, &ctx, Server.acceptCb);

    try loop.run(.until_done);
}
