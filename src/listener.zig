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
        std.mem.copy(u8, node.data.payload[0..rlen], buf[0..rlen]);
        node.data.len = rlen;
        // send it for processing
        context.q.put(node);
    }
}
