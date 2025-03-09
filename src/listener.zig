const std = @import("std");
const builtin = @import("builtin");

const Metric = @import("metric.zig").Metric;
const MetricType = @import("metric.zig").MetricType;
const Sampler = @import("sampler.zig").Sampler;
const ThreadContext = @import("main.zig").ThreadContext;

pub const Packet = struct {
    payload: [8192]u8,
    len: usize,
};

fn open_socket_udp() !i32 {
    const sockfd: i32 = try std.posix.socket(
        std.posix.AF.INET,
        std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK,
        0,
    );
    var addr: std.net.Address = try std.net.Address.parseIp4("127.0.0.1", 8125);
    try std.posix.bind(sockfd, &addr.any, @sizeOf(std.posix.sockaddr.in));
    return sockfd;
}

fn open_socket_uds() !i32 {
    const sockfd: i32 = try std.posix.socket(
        std.posix.AF.UNIX,
        std.posix.SOCK.DGRAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK,
        0,
    );
    var addr: std.net.Address = try std.net.Address.initUnix("statsd.sock");
    try std.posix.bind(sockfd, &addr.any, @sizeOf(std.posix.sockaddr.in));
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
    const sockfd: i32 = try open_socket(context);

    // reading buffer
    var array: [8192]u8 = undefined;
    var buf: []u8 = &array;

    var drops: u64 = 0;
    var last_drop_message = std.time.milliTimestamp();

    // initialization
    // epoll
    var epfd: usize = undefined;
    // kqueue
    var kq: i32 = undefined;
    var kev: switch (builtin.os.tag) {
        .macos => std.posix.Kevent,
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
            try std.posix.epoll_ctl(
                @as(i32, @intCast(epfd)),
                std.os.linux.EPOLL.CTL_ADD,
                sockfd,
                &epev,
            );
            errdefer std.posix.linux.close(epfd);
        },
        .netbsd, .openbsd, .macos => {
            std.log.info("using kqueue", .{});
            kq = try std.posix.kqueue();
            errdefer std.posix.close(kq);
            kev = std.posix.Kevent{
                .ident = @intCast(sockfd),
                .filter = std.c.EVFILT.READ,
                .flags = std.c.EV.CLEAR | std.c.EV.ADD | std.c.EV.DISABLE | std.c.EV.ONESHOT,
                .fflags = std.c.NOTE.CRITICAL,
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
                var events: [10]std.os.linux.epoll_event = undefined;
                _ = std.os.linux.epoll_wait(@as(i32, @intCast(epfd)), events[0..], 10, -1);
                // std.log.info("epoll events count: {d}", .{events_count});
            },
            .netbsd, .openbsd, .macos => {
                const empty_kevs = &[0]std.posix.Kevent{};
                const kevent_array = @as(*const [1]std.posix.Kevent, &kev);
                _ = try std.posix.kevent(kq, kevent_array, empty_kevs, null);
            },
            else => {},
        }

        const rlen = std.posix.recvfrom(sockfd, buf, 0, null, null) catch {
            std.posix.nanosleep(0, 1000000);
            continue;
        };

        if (rlen == 0) {
            std.posix.nanosleep(0, 1000000);
            continue;
        }

        const maybe_node = context.b.get();
        if (maybe_node == null) {
            drops += 1;
            // no more pre-allocated buffers available, this packet will be dropped.
            continue;
        }

        // take a pre-allocated buffers
        var node = maybe_node.?;
        // copy the data
        std.mem.copyForwards(u8, node.data.payload[0..rlen], buf[0..rlen]);
        node.data.len = rlen;

        // send it for processing
        context.q.put(node);

        const tmp: u64 = @intCast(std.time.milliTimestamp() - last_drop_message);
        if (tmp > 10000) {
            last_drop_message = std.time.milliTimestamp();
            std.log.info("listener drops: {d}/s ({d} last {d}s)", .{ @divTrunc(drops, @divTrunc(tmp, 1000)), drops, 10 });

            // FIXME(remy): just because of this call, the main Sampler isn't thread safe
            // and has to lock its maps. Either the sampler_telemetry should have a thread
            // safe path (with sampleDist and sampleSerie with a lock parameter) or it should
            // not be emitted from the listener thread.
            context.sampler.sampleTelemetry(.Counter, "statsd.listener.packets_drop", @floatFromInt(drops), .empty);

            drops = 0;
        }

        //        std.posix.nanosleep(0, 100000);
    }
}
