const std = @import("std");

// configuration

const min_metrics_per_s = 400000;
const payload_max_size = 8192;

fn connectUnixSocket(path: []const u8) !std.net.Stream {
    const sockfd = try std.posix.socket(
        std.posix.AF.UNIX,
        std.posix.SOCK.DGRAM,
        0,
    );

    errdefer std.net.Stream.close(.{ .handle = sockfd });

    var addr = try std.net.Address.initUnix(path);
    try std.posix.connect(sockfd, &addr.any, addr.getOsSockLen());

    return .{ .handle = sockfd };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const socket_path_opt = std.posix.getenv("UDS_SOCKET");
    var socket_path: []const u8 = undefined;
    if (socket_path_opt) |sp| {
        socket_path = sp;
    } else {
        std.log.info("Use UDS_SOCKET env var to provide the path to the UDS socket", .{});
        return;
    }

    const stream = try connectUnixSocket(socket_path);

    const payload, const metrics_per_packet = try build_payload(allocator);
    defer allocator.free(payload.items);

    // we want sleep 1ms at least every other 1ms, meaning that in one second (1000ms)
    // we have only 500ms to send metrics.
    // this will linearly send the traffic instead of sending burst
    // of packets in 1 or 2ms, which can't be processed
    // properly by a server.
    const target_rate_per_ms: f64 = min_metrics_per_s / 500;
    std.log.debug("will have to send {d} metrics/ms", .{target_rate_per_ms});
    const packets_to_send_per_cycle: u64 = 1 + @as(u64, @intFromFloat(target_rate_per_ms)) / metrics_per_packet;
    std.log.debug("meaning we'll have to send {d} packets per loop iteration (before a 1ms sleep)", .{packets_to_send_per_cycle});


    while (true) {
        const start = std.time.milliTimestamp();
        std.log.debug("cycle starting", .{});

        var sent: u64 = 0;
        while (sent < min_metrics_per_s) {
            const sub_start = std.time.microTimestamp();

            var packets_to_send = packets_to_send_per_cycle;
            while (packets_to_send > 0) : (packets_to_send -= 1) {
                try stream.writeAll(payload.items);
                sent += metrics_per_packet;
            }

            const sub_end = std.time.microTimestamp();
            if (sub_end-sub_start < std.time.ns_per_ms) {
                // it look less than 1ms to send the metrics, we can oversleep
                // by 1ms.
                std.time.sleep(1*std.time.ns_per_ms);
            }
            std.time.sleep(1*std.time.ns_per_ms);
        }

        const end = std.time.milliTimestamp();
        if (end-start < 1000) {
            const sleep_left: u64 = @intCast(1000 - (end-start));
            std.log.debug("cycle ending, sent {d} metrics in {d}ms, will sleep {d}ms", .{sent, end-start, sleep_left});
            std.time.sleep(sleep_left*std.time.ns_per_ms);
        } else {
            std.log.debug("can't sleep we're too slow! gotta go fast!", .{});
        }
    }

    stream.close();
}

fn build_payload(allocator: std.mem.Allocator) !struct{std.ArrayListUnmanaged(u8), u64} {
    var rv = std.ArrayListUnmanaged(u8).empty;
    const count = "my.count:25|c|#environment:dev,dev:remeh,service:zig\n";
    const dist = "my.dist:50|d|#environment:dev,dev:remeh,service:zig\n";

    var metrics_per_packet: usize = 0;

    while (true) {
        if (rv.items.len + count.len < payload_max_size) {
            try rv.appendSlice(allocator, count);
            metrics_per_packet += 1;
        } else {
            break;
        }

        if (rv.items.len + dist.len < payload_max_size) {
            try rv.appendSlice(allocator, dist);
            metrics_per_packet += 1;
        } else {
            break;
        }
    }

    std.log.debug("one packet will contain {d} metrics", .{metrics_per_packet});

    return .{rv, metrics_per_packet};
}