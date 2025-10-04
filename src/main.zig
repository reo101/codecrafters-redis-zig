const std = @import("std");
const net = std.net;
const Reader = std.io.Reader;
const Writer = std.io.Writer;

/// Parse a line ending in "\r\n", consuming them
fn readLineCRLF(reader: *Reader, scratch: []u8) ![]u8 {
    var writer: Writer = .fixed(scratch);

    const n = try Reader.streamDelimiter(reader, &writer, '\n');
    // NOTE: `toss` the remaining `\n`
    reader.toss(1);
    if (scratch[n - 1] != '\r') {
        return error.NoCarriageReturn;
    }

    const line = scratch[0 .. n - 1];
    if (line.len == 0) {
        return error.EmptyCommand;
    }

    return line;
}

/// Read exactly "\r\n", used when we've exactly read all data before that
fn readExactCRLF(reader: *Reader) !void {
    var crlf = std.mem.zeroes([2]u8);
    const n = try reader.readSliceShort(&crlf);
    if (n != 2 or !std.mem.eql(u8, &crlf, "\r\n")) return error.BadCRLF;
}

fn readNBytesAlloc(reader: *Reader, allocator: std.mem.Allocator, n: usize) ![]u8 {
    var buf = try allocator.alloc(u8, n);
    var filled: usize = 0;
    while (filled < n) {
        const got = try reader.readSliceShort(buf[filled..]);
        if (got == 0) return error.UnexpectedEof;
        filled += got;
    }
    return buf;
}

/// Parse a single RESP array of bulk strings: "*N\r\n$<len>\r\n<bytes>\r\n"
pub fn readRespCommand(reader: *Reader, allocator: std.mem.Allocator) !struct {
    argv: []const []u8,
    arena: std.heap.ArenaAllocator,
} {
    var line_buf: [1 << 13]u8 = undefined;

    // NOTE: 1) Array header:
    //          "*3\r\n"
    const header = try readLineCRLF(reader, &line_buf);
    if (header.len == 0 or header[0] != '*') return error.NotAnArray;
    const count = try std.fmt.parseInt(usize, header[1..], 10);

    std.debug.print("Header: {d} commands\n", .{count});

    var arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();

    // WARN: parent `allocator` owns the list 
    var parts: std.ArrayList([]u8) = try .initCapacity(allocator, count);

    // NOTE: 2) For each element: expect a bulk string:
    //          "$<len>\r\n<bytes>\r\n"
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const typeline = try readLineCRLF(reader, &line_buf);
        if (typeline.len == 0) return error.BadFrame;

        switch (typeline[0]) {
            // Bulk string
            '$' => {
                const len = try std.fmt.parseInt(usize, typeline[1..], 10);
                if (len == 0xffffffffffffffff) return error.BadLength;

                // Read exactly len bytes, then a CRLF
                // WARN: child `arena` owns the elements
                const data = try readNBytesAlloc(reader, a, len);
                try readExactCRLF(reader);

                try parts.append(allocator, data);
            },
            else => return error.UnsupportedType,
        }
    }

    return .{
        .argv = try parts.toOwnedSlice(a),
        .arena = arena,
    };
}

const stdout = std.fs.File.stdout();
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const alloc = gpa.allocator();

    try stdout.writeAll("Logs from your program will appear here!\n");

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var input_buff = std.mem.zeroes([128]u8);
    var output_buff = std.mem.zeroes([128]u8);

    while (true) {
        const connection = try listener.accept();
        defer connection.stream.close();

        try stdout.writeAll("Accepted new connection\n");

        var stream_reader: net.Stream.Reader = connection.stream.reader(&input_buff);
        const reader: *Reader = stream_reader.interface();

        var stream_writer: net.Stream.Writer = connection.stream.writer(&output_buff);
        const writer: *Writer = &stream_writer.interface;

        const cmd = try readRespCommand(reader, alloc);
        defer cmd.arena.deinit();
        defer alloc.free(cmd.argv);

        for (cmd.argv, 0..) |arg, idx| {
            std.debug.print("{d}: {s}\n", .{ idx, arg });
            if (std.mem.eql(u8, arg, "PING")) {
                _ = try writer.write("+PONG\r\n");
            }
            try writer.flush();
        }
    }
}
