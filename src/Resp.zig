const std = @import("std");
const net = std.net;
const io = std.io;

const Server = @import("./Server.zig");

const log = std.log.scoped(.Resp);

pub fn writeBulkInto(buf: []u8, string_opt: ?[]const u8) []const u8 {
    if (string_opt) |string| {
        const header = std.fmt.bufPrint(buf, "${d}\r\n", .{string.len}) catch unreachable;
        var i: usize = header.len;
        std.mem.copyForwards(u8, buf[i .. i + string.len], string);
        i += string.len;
        buf[i] = '\r';
        buf[i + 1] = '\n';
        i += 2;
        return buf[0..i];
    } else {
        const nil = "$-1\r\n";
        std.mem.copyForwards(u8, buf[0..nil.len], nil);
        return buf[0..nil.len];
    }
}

pub const FmtArgv = struct {
    argv: []const []const u8,
    pub fn format(self: @This(), writer: anytype) !void {
        try writer.writeByte('[');
        for (self.argv, 0..) |arg, i| {
            if (i != 0) try writer.writeAll(", ");
            try writer.print("\"{s}\"", .{arg});
        }
        try writer.writeByte(']');
    }
};

pub const Parser = struct {
    pub const MAX_ARGV = 16;

    /// Rolling buffer that survives across reads
    in_buf: [8192]u8 = undefined,
    in_used: usize = 0,

    /// `argv` scratch (slices point into `in_buf`)
    argv: [MAX_ARGV][]const u8 = undefined,
    argc: usize = 0,
};

pub const Err = error{
    NeedMore,
    NotArray,
    BadCRLF,
    BadLen,
    BadInt,
    UnsupportedType,
};

fn findCRLF(s: []const u8) ?usize {
    if (s.len < 2) {
        return null;
    }
    var i: usize = 1;
    while (i < s.len) : (i += 1) {
        if (s[i] == '\n' and s[i - 1] == '\r') {
            return i - 1;
        }
    }
    return null;
}

fn readLineCRLF_mem(buf: []const u8, start: usize) !struct { line: []const u8, next: usize } {
    const rest = buf[start..];
    const cr = findCRLF(rest) orelse return Err.NeedMore;
    return .{
        .line = rest[0..cr],
        .next = start + cr + 2,
    };
}

/// Try to parse ONE RESP array-of-bulk-strings from parser.in_buf
/// On success: sets parser.argv/argc and returns bytes_consumed
/// On partial input: returns error.NeedMore (do not modify the buffer)
pub fn tryParseOneInPlace(p: *Parser) !usize {
    const src = p.in_buf[0..p.in_used];
    if (src.len == 0) {
        return Err.NeedMore;
    }

    var pos: usize = 0;

    const header = try readLineCRLF_mem(src, pos);
    if (header.line.len == 0 or header.line[0] != '*') {
        return Err.NotArray;
    }
    const count = std.fmt.parseInt(usize, header.line[1..], 10) catch return Err.BadInt;
    if (count > Parser.MAX_ARGV) {
        return Err.BadLen;
    }
    pos = header.next;

    p.argc = 0;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const tl = try readLineCRLF_mem(src, pos);
        if (tl.line.len == 0 or tl.line[0] != '$') {
            return Err.UnsupportedType;
        }
        const blen = std.fmt.parseInt(usize, tl.line[1..], 10) catch return Err.BadInt;
        pos = tl.next;

        if (src.len < pos + blen + 2) {
            return Err.NeedMore;
        }
        const body = src[pos .. pos + blen];
        pos += blen;

        if (src[pos] != '\r' or src[pos + 1] != '\n') {
            return Err.BadCRLF;
        }
        pos += 2;

        p.argv[i] = body;
        p.argc += 1;
    }
    return pos;
}

/// Slide remaining bytes down after consuming `n`
pub fn compact(p: *Parser, n: usize) void {
    if (n == 0) {
        return;
    }
    if (n < p.in_used) {
        @memmove(p.in_buf[0..], p.in_buf[n..p.in_used]);
    }
    p.in_used -= n;
}

pub const State = struct {
    kv: std.StringArrayHashMapUnmanaged([]u8),
    allocator: std.mem.Allocator,
};
