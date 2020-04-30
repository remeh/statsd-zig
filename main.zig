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
    // packets read from the network
    q: std.atomic.Queue([]u8),
    // buffers available to share data between the listener thread
    // and the parser thread.
    b: std.atomic.Queue([]u8)
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

    // reading buffer
    var array: [8192]u8 = undefined;
    var buf: []u8 = &array;

    while (true) {
        const rlen = os.recvfrom(sockfd, buf, 0, null, null) catch {
            continue;
        };
        if (rlen == 0) {
            continue;
        }

        if (context.b.isEmpty()) {
            // no more pre-allocated buffers available, this packet will be dropped.
            continue;
        }

        // take a pre-allocated buffers
        var node = context.b.get().?;

        // copy the data
        var i: usize = 0;
        while (i < 8192) {
            node.data[i] = buf[i];
            if (buf[i] == 0) { break; }
            buf[i] = 0;
            i += 1;
        }

        // send it for processing
        context.q.put(node);
    }
}

pub fn main() !void {
    // queue communicating packets to parse
    var queue = Queue([]u8).init();

    // pre-alloc 65535 buffers that will be re-used to contain the read data
    // these buffers will do round-trips between the listener and the parser
    var buffers = Queue([]u8).init();
    var i: usize = 0;
    while (i < 65535) { // 512MB
        var node: *Queue([]u8).Node = try allocator.create(Queue([]u8).Node);
        node.data = try allocator.alloc(u8, 8192);
        buffers.put(node);
        i += 1;
    }

    // shared context
    var tx = ThreadContext{
      .q = queue,
      .b = buffers,
    };

    // spawn the listening thread
    var listener_thread = std.Thread.spawn(&tx, listener);

    // mainloop
    while (true) {
        while (!tx.q.isEmpty()) {
            var node = tx.q.get().?;

            if (Parser.parse_packet(node.data)) |metrics| {
//                warn("# metrics parsed: {}\n", .{metrics.span().len});
            } else |err| {
                warn("can't parse packet: {}\n", .{err});
                warn("packet: {}\n", .{node.data});
            }

            // send this buffer back to the usable queue of buffers
            tx.b.put(node);
        }
    }
}

