const std = @import("std");
const builtin = @import("builtin");
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

fn open_socket(context: *ThreadContext) !i32 {
    var sockfd: i32 = 0;

    if (context.uds) {
        sockfd = try open_socket_uds();
        std.log.info("starting the listener on statsd.sock", .{});
    } else {
        sockfd = try open_socket_udp();
        std.log.info("starting the listener on localhost:8125", .{});
    }

    return sockfd;
}

pub fn listener(context: *ThreadContext) !void {
    var sockfd: i32 = try open_socket(context);

    // reading buffer
    var array: [8192]u8 = undefined;
    var buf: []u8 = &array;

    var drops: i64 = 0;
    var last_drop_message = std.time.milliTimestamp();

    // initialization
    // epoll
    var epfd: usize = undefined;
    // kqueue
    var kq: i32 = undefined;
    var kev: switch (builtin.os.tag) {
        .macos => std.os.Kevent,
        else => usize,
    } = undefined;

    switch (builtin.os.tag) {
        .linux => {
            // epoll
            std.log.info("using epoll", .{});
            epfd = std.os.linux.epoll_create();
            var epev = std.os.linux.epoll_event{
                .events = std.os.linux.EPOLL.IN | std.os.linux.EPOLL.PRI | std.os.linux.EPOLL.ERR | std.os.linux.EPOLL.HUP,
                .data = std.os.linux.epoll_data{ .fd = sockfd },
            };
            try std.os.epoll_ctl(
                @intCast(i32, epfd),
                std.os.linux.EPOLL.CTL_ADD,
                sockfd,
                &epev,
            );
            errdefer std.os.linux.close(epfd);
        },
        .netbsd, .openbsd, .macos => {
            std.log.info("using kqueue", .{});
            kq = try std.os.kqueue();
            errdefer std.os.close(kq);
            kev = std.os.Kevent{
                .ident = 1,
                .filter = os.system.EVFILT_TIMER,
                .flags = os.system.EV_CLEAR | os.system.EV_ADD | os.system.EV_DISABLE | os.system.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = undefined,
                // .udata = @ptrToInt(&eventfd_node.data.base),
            };
        },
        else => {},
    }

    // process loop
    while (true) {
        switch (builtin.os.tag) {
            .linux => {
                var events: [10]os.linux.epoll_event = undefined;
                _ = std.os.linux.epoll_wait(@intCast(i32, epfd), events[0..], 10, -1);
                // std.log.info("epoll events count: {d}", .{events_count});
            },
            .netbsd, .openbsd, .macos => {
                // TODO(remy): kqueue implementation
                const empty_kevs = &[0]os.Kevent{};
                const kevent_array = @as(*const [1]os.Kevent, &kev);
                _ = try std.os.kevent(kq, kevent_array, empty_kevs, null);
            },
            else => {},
        }

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
            std.log.info("listener drops: {d}/s ({d} last {d}s)", .{ @divTrunc(drops, @divTrunc(tmp, 1000)), drops, 10 });
            drops = 0;
        }
    }
}
