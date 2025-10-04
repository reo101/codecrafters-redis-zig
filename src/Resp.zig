const std = @import("std");
const net = std.net;
const io = std.io;

const log = std.log.scoped(.RespCommand);

/// Parse a line ending in "\r\n", consuming them
fn readLineCRLF(reader: *io.Reader, scratch: []u8) ![]u8 {
    @memset(scratch, 0);
    var writer: io.Writer = .fixed(scratch);

    const n = try io.Reader.streamDelimiter(reader, &writer, '\n');
    // NOTE: `toss` the remaining `\n`
    reader.toss(1);
    if (n == 0 or scratch[n - 1] != '\r') {
        log.err("scratch: {s}", .{scratch});

        return error.NoCarriageReturn;
    }

    const line = scratch[0 .. n - 1];
    if (line.len == 0) {
        return error.EmptyCommand;
    }

    return line;
}

/// Read exactly "\r\n", used when we've exactly read all data before that
fn readExactCRLF(reader: *io.Reader) !void {
    var crlf = std.mem.zeroes([2]u8);
    const n = try reader.readSliceShort(&crlf);
    if (n != 2 or !std.mem.eql(u8, &crlf, "\r\n")) {
        log.err("CRLF: {d} {d}, instead of {d} {d}", .{ crlf[0], crlf[1], '\r', '\n' });
        return error.BadCRLF;
    }
}

fn readNBytesAlloc(reader: *io.Reader, allocator: std.mem.Allocator, n: usize) ![]u8 {
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
pub fn readOneCommand(reader: *io.Reader, allocator: std.mem.Allocator) !struct {
    /// Slice of strings
    argv: []const []u8,
    /// Arena allocator for the separate commands
    arena: std.heap.ArenaAllocator,
} {
    var line_buf: [1 << 13]u8 = undefined;

    // NOTE: 1) Array header:
    //          "*3\r\n"
    const header = try readLineCRLF(reader, &line_buf);
    if (header.len == 0 or header[0] != '*') return error.NotAnArray;
    const count = try std.fmt.parseInt(usize, header[1..], 10);

    log.debug("Header: {d} commands", .{count});

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

        log.debug("Typeline #{d}: {s}", .{ i, typeline });

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
        .argv = try parts.toOwnedSlice(allocator),
        .arena = arena,
    };
}

pub fn handleConnection(reader: *io.Reader, writer: *io.Writer, allocator: std.mem.Allocator) !void {
    while (true) {
        const cmd = readOneCommand(reader, allocator) catch |err| switch (err) {
            // NOTE: connection closed (expectedly), exit the per-connection loop
            error.EndOfStream,
            => {
                break;
            },
            else => return err,
        };
        defer cmd.arena.deinit();
        defer allocator.free(cmd.argv);

        for (cmd.argv, 0..) |arg, idx| {
            log.debug("{d}: {s}", .{ idx, arg });

            if (std.mem.eql(u8, arg, "PING")) {
                _ = try writer.write("+PONG\r\n");
            }
            try writer.flush();
        }
    }
}
