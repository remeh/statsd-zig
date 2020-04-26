const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const os = std.os;
const net = std.net;
const assert = std.debug.assert;
const warn = std.debug.warn;
const allocator = std.allocator;

const Parser = @import("parser.zig").Parser;
const Metric = @import("metric.zig").Metric; 

// this method receives a metric and its value such as:
// custom_metric:50.1
//pub fn separate_name_value(metric: []u8) Metric {
//}

//pub fn parse_packet(slice: []u8) !void {
//}

fn open_socket() !i32 {
    var sockfd: i32 = try os.socket(
        os.AF_INET,
        os.SOCK_DGRAM | os.SOCK_CLOEXEC | os.SOCK_NONBLOCK,
        0,
    );
    var addr: net.Address = try net.Address.parseIp4("127.0.0.1", 8125);
    try os.bind(sockfd, &addr.any, @sizeOf(os.sockaddr_in));
    return sockfd;
}

fn read_packet(sockfd: i32) ?[]u8 {
    var buf: [1500]u8 = undefined;
    var bufSlice: []u8 = &buf;
    var sl: u32 = @sizeOf(os.sockaddr_in);
    const rlen = os.recvfrom(sockfd, bufSlice, 0, null, null) catch return null;
    if (rlen == 0) {
        return null;
    }

    return bufSlice;
}

pub fn main() !void {
    var sockfd: i32 = try open_socket();
    // mainloop
    while (true) {
        var buf = read_packet(sockfd);
        if (buf == null) { continue; }
        _ = async Parser.parse_packet(buf.?);
//        if (await frame) |metrics| {
//            warn("parsed metrics: {}\n", .{metrics.span()});
//        } else |err| {
//            warn("can't parse packet: {}\n", .{err});
//            warn("packet: {}\n", .{bufSlice});
//        }
    }
}

