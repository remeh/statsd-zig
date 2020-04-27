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

const ThreadContext = struct {
    q: std.atomic.Queue([]u8)
};

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

fn listener(context: *ThreadContext) !void {
    var sockfd: i32 = try open_socket();
    std.debug.warn("starting the listener on 127.0.0.1:8125\n", .{});
    while (true) {
        var buf: [1500]u8 = undefined;
        var bufSlice: []u8 = &buf;
        var sl: u32 = @sizeOf(os.sockaddr_in);
        const rlen = os.recvfrom(sockfd, bufSlice, 0, null, null) catch continue;
        if (rlen == 0) {
            continue;
        }
        std.debug.warn("received {}\n", .{bufSlice});
        // FIXME(remy): it is properly recceived here, however, that is most likely
        // that the std.atomic.Queue is storing the data on the stack and the other
        // thread can't read the value? std.event.Channel may be the way to go, or not.
        var entry = std.atomic.Queue([]u8).Node{
            .data = bufSlice,
        };
        context.q.put(&entry);
        // TODO(remy): write bufSlice in a queue
    }
}

pub fn main() !void {
    var queue = std.atomic.Queue([]u8).init();
    var tx = ThreadContext{ .q = queue };
    var listener_thread = std.Thread.spawn(&tx, listener);
    // mainloop
    while (true) {
        std.debug.warn("parse some packets\n", .{});
        while (!tx.q.isEmpty()) {
            var buf = tx.q.get();
            if (Parser.parse_packet(buf.?.data)) |metrics| {
                warn("parsed metrics: {}\n", .{metrics.span()});
            } else |err| {
                warn("can't parse packet: {}\n", .{err});
                warn("packet: {}\n", .{buf.?.data});
            }
        }
        std.time.sleep(1000000000);
    }
}

