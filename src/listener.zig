const std = @import("std");
const os = std.os;

const ThreadContext = @import("main.zig").ThreadContext;

pub const Packet = struct {
    payload: []u8,
    len: usize,
};

fn open_socket_udp() !i32 {
    var sockfd: i32 = try os.socket(
        os.AF.INET,
        os.SOCK.DGRAM | os.SOCK.CLOEXEC | os.SOCK.NONBLOCK,
        0,
    );
    var addr: std.net.Address = try std.net.Address.parseIp4("127.0.0.1", 8125);
    try os.bind(sockfd, &addr.any, @sizeOf(os.sockaddr.in));
    return sockfd;
}

fn open_socket_uds() !i32 {
    var sockfd: i32 = try os.socket(
        os.AF.UNIX,
        os.SOCK.DGRAM | os.SOCK.CLOEXEC | os.SOCK.NONBLOCK,
        0,
    );
    var addr: std.net.Address = try std.net.Address.initUnix("statsd.sock");
    try os.bind(sockfd, &addr.any, @sizeOf(os.sockaddr.in));
    return sockfd;
}

pub fn listener(context: *ThreadContext) !void {
    var sockfd: i32 = 0;

    if (context.uds) {
        sockfd = try open_socket_uds();
        std.log.info("starting the listener on statsd.sock", .{});
    } else {
        sockfd = try open_socket_udp();
        std.log.info("starting the listener on localhost:8125", .{});
    }

    // reading buffer
    var array: [8192]u8 = undefined;
    var buf: []u8 = &array;

    var drops: i64 = 0;
    var last_drop_message = std.time.milliTimestamp();

    while (true) {
        std.os.nanosleep(0, 100 * 1000 * 1000);

        const rlen = os.recvfrom(sockfd, buf, 0, null, null) catch {
            continue;
        };
        if (rlen == 0) {
            continue;
        }
        if (context.b.isEmpty()) {
            drops += 1;
            // no more pre-allocated buffers available, this packet will be dropped.
            continue;
        }

        // take a pre-allocated buffers
        var node = context.b.get().?;
        // copy the data
        std.mem.copy(u8, node.data.payload[0..rlen], buf[0..rlen]);
        node.data.len = rlen;
        // send it for processing
        context.q.put(node);

        const tmp = std.time.milliTimestamp() - last_drop_message;
        if (tmp > 10000) {
            last_drop_message = std.time.milliTimestamp();
            std.log.info("drops: {}/s", .{@divTrunc(drops, @divTrunc(tmp, 1000))});
            drops = 0;
        }
    }
}
