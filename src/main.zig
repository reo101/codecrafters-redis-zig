const Resp = @import("./Resp.zig");

const std = @import("std");
const net = std.net;
const io = std.io;

const log = std.log.scoped(.main);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    log.info("Program started", .{});

    const port: u16 = 6379;

    const address: net.Address = .initIp4(.{ 127, 0, 0, 1 }, port);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    log.info("Listening at port {d}", .{port});

    var input_buff = std.mem.zeroes([128]u8);
    var output_buff = std.mem.zeroes([128]u8);

    while (true) {
        const connection = try listener.accept();
        defer connection.stream.close();

        log.info("Accepted new connection", .{});

        var stream_reader: net.Stream.Reader = connection.stream.reader(&input_buff);
        const reader: *io.Reader = stream_reader.interface();

        var stream_writer: net.Stream.Writer = connection.stream.writer(&output_buff);
        const writer: *io.Writer = &stream_writer.interface;

        try Resp.handleConnection(reader, writer, allocator);
    }
}
