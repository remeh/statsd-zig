const std = @import("std");
const warn = std.debug.warn;
const os = std.os;

const ThreadContext = @import("main.zig").ThreadContext;

pub const Packet = struct {
    payload: []u8, len: usize
};

fn open_socket() !i32 {
    var sockfd: i32 = try os.socket(
        os.AF_INET,
        os.SOCK_DGRAM | os.SOCK_CLOEXEC | os.SOCK_NONBLOCK,
        0,
    );
    var addr: std.net.Address = try std.net.Address.parseIp4("127.0.0.1", 8125);
    try os.bind(sockfd, &addr.any, @sizeOf(os.sockaddr_in));
    return sockfd;
}

pub fn listener(context: *ThreadContext) !void {
    var sockfd: i32 = try open_socket();
    warn("starting the listener on 127.0.0.1:8125\n", .{});

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
        warn("received: {}\n", .{node.data});
        // send it for processing
        context.q.put(node);

        const tmp = std.time.milliTimestamp() - last_drop_message;
        if (tmp > 10000) {
            last_drop_message = std.time.milliTimestamp();
            warn("drops: {}/s\n", .{@divTrunc(drops, @divTrunc(tmp, 1000))});
            drops = 0;
        }
    }
}
