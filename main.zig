const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const os = std.os;
const net = std.net;
const assert = std.debug.assert;
const warn = std.debug.warn;
const allocator = std.heap.page_allocator;
const Queue = std.atomic.Queue;

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
    warn("starting the listener on 127.0.0.1:8125\n", .{});
    var buf = try allocator.alloc(u8, 8192);
    while (true) {
        const rlen = os.recvfrom(sockfd, buf, 0, null, null) catch {
            continue;
        };
        if (rlen == 0) {
            continue;
        }

        var copy = try allocator.alloc(u8, 8192);
        var i: usize = 0;
        while (i < 8192) {
            copy[i] = buf[i];
            i += 1;
        }

        var node: []Queue([]u8).Node = try allocator.alloc(Queue([]u8).Node, 1);
        node[0].data = copy;
        context.q.put(&node[0]);
    }
}

pub fn main() !void {
    var queue = Queue([]u8).init();
    var tx = ThreadContext{ .q = queue };
    var listener_thread = std.Thread.spawn(&tx, listener);
    // mainloop
    while (true) {
        warn("parse some packets\n", .{});
        while (!tx.q.isEmpty()) {
            var node = tx.q.get().?;
            warn("got from the queue: {}\n", .{node.data});
//            if (Parser.parse_packet(buf.?.data)) |metrics| {
//                warn("parsed metrics: {}\n", .{metrics.span()});
//            } else |err| {
//                warn("can't parse packet: {}\n", .{err});
//                warn("packet: {}\n", .{buf.?.data});
//            }
            // TODO(remy): release the memory
            allocator.free(node.data);
        }
        std.time.sleep(1000000000);
    }
}

