const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const os = std.os;
const net = std.net;
const assert = std.debug.assert;
const warn = std.debug.warn;
const allocator = std.allocator;

const METRIC_TYPE_GAUGE: u8   = 'g';
const METRIC_TYPE_COUNTER: u8 = 'c';

const Metric = struct {
        name: ?[]const u8,
        value: f32,
        type: u8,
        // TODO(remy): tags
};

// this method receives a metric and its value such as:
// custom_metric:50.1
pub fn separate_name_value(metric: []u8) void {
    // TODO(remy):
}

pub fn parse_packet(slice: []u8) !void {
        var iterator = std.mem.split(slice, "|");
        const part: ?[]const u8 = iterator.next();
        var idx: u8 = 0;
        var m: Metric = Metric{
          .name = "",
          .value = 0.0,
          .type = METRIC_TYPE_GAUGE,
        };
        while (part != null) {
                if (idx == 0) {
                    
                } else if (idx == 1) {
                    //TODO(remy): type
                }
                idx += 1;
                warn("part: {}\n", .{part});
                part = iterator.next();
        }
        warn("we've received a metric named {} value {}", .{m.name, m.value});
}

pub fn main() !void {
        var sockfd: i32 = try os.socket(
                os.AF_INET,
                os.SOCK_DGRAM | os.SOCK_CLOEXEC | os.SOCK_NONBLOCK,
                0,
        );
        var addr: net.Address = try net.Address.parseIp4("127.0.0.1", 8125);

        try os.bind(sockfd, &addr.any, @sizeOf(os.sockaddr_in));
        while (true) {
                // 1500 because an ethernet packet is 1500 max
                var buf: [1500]u8 = undefined;
                var bufSlice: []u8 = &buf;
                var sl: u32 = @sizeOf(os.sockaddr_in);
                const rlen = os.recvfrom(sockfd, bufSlice, 0, null, null) catch continue;
                if (rlen > 0) {
                        warn("content: {}", .{buf});
                        try parse_packet(bufSlice);
                }
                std.time.sleep(1000);
        }
}
