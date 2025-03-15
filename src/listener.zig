const std = @import("std");
const builtin = @import("builtin");

const AtomicQueue = @import("atomic_queue.zig").AtomicQueue;
const Event = @import("event.zig").Event;
const EventType = @import("event.zig").EventType;
const PreallocatedPool = @import("preallocated_packets_pool.zig").PreallocatedPool;
const Sampler = @import("sampler.zig").Sampler;
const Signal = @import("signal.zig").Signal;

pub const Packet = struct {
    payload: [8192]u8,
    len: usize,
};

const ListenerConfig = struct {
    packets_pool_size: usize,
    uds: bool,
};

/// Listener spawns a separate thread to listen for packets.
/// They're communicated to the main thread through the `ListenerThread.q` atomic queue,
/// a signal in `ListenerThread.packets_signal` is sent when the main thread should look
/// into the queue to process something. Once the main thread is done with the packet,
/// it should be pushed back in the `ListenerThread.b` pre-allocated pool of packets.
pub const Listener = struct {
    gpa: std.mem.Allocator,
    pthread: std.Thread,
    thread: *ListenerThread,
    packets_pool: *PreallocatedPool(Packet),

    pub fn init(gpa: std.mem.Allocator, sampler: *Sampler, listener_config: ListenerConfig) !Listener {
        var listener = Listener{
            .gpa = gpa,
            .pthread = undefined,
            .thread = undefined,
            .packets_pool = undefined,
        };

        // pre-alloc a given amout of packets that will be re-used to contain the read data,
        // these packets will do round-trips between the listener thread and the main thread.
        listener.packets_pool = try PreallocatedPool(Packet).init(
            gpa,
            listener_config.packets_pool_size,
            Packet{
                .payload = std.mem.zeroes([8192]u8),
                .len = 0,
            },
        );

        const thread_context = try gpa.create(ListenerThread);
        thread_context.* = .{
            .q = AtomicQueue(Packet).init(),
            .b = listener.packets_pool,
            .running = std.atomic.Value(bool).init(true),
            .uds = listener_config.uds,
            .packets_signal = try Signal.init(),
            .sampler = sampler,
        };

        // spawn the thread
        listener.pthread = try std.Thread.spawn(std.Thread.SpawnConfig{}, ListenerThread.run, .{thread_context});
        listener.thread = thread_context;

        return listener;
    }

    pub fn deinit(self: *Listener) void {
        self.thread.running.store(false, .release);
        self.pthread.join();
        self.packets_pool.deinit();
        while (self.thread.q.get()) |node| {
            self.gpa.destroy(node);
        }
        self.gpa.destroy(self.thread);
    }
};

/// ListenerThread is the separate thread listening on sockets and sending
/// packets to process to the main thread.
pub const ListenerThread = struct {
    /// packets read from the network waiting to be processed
    q: AtomicQueue(Packet),
    /// packets buffers available to share data between the listener thread
    /// and the parser thread.
    b: *PreallocatedPool(Packet),
    /// is running in UDS
    uds: bool,
    /// used by the listener thread that something has been put for processing
    /// in the queue.
    packets_signal: Signal,
    /// sampler to send health telemetry from the server itself
    sampler: *Sampler,

    running: std.atomic.Value(bool),

    pub fn run(self: *ListenerThread) !void {
        const sockfd: i32 = try self.open_socket();

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
                    @intCast(epfd),
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
            },
            else => {},
        }

        // process loop
        var running = true;
        while (running) {
            // wait for an event from epoll/kqueue notifying that there is something
            // to read on the socket
            // also, every 2s, leave the wait to see if the thread should stop.
            switch (builtin.os.tag) {
                .linux => {
                    var events: [10]std.os.linux.epoll_event = undefined;
                    _ = std.os.linux.epoll_wait(@intCast(epfd), events[0..], 10, 2000);
                },
                .netbsd, .openbsd, .macos => {
                    const event: std.posix.Kevent = .{
                        .ident = @intCast(sockfd),
                        .filter = std.posix.system.EVFILT.READ,
                        .flags = std.posix.system.EV.ADD | std.posix.system.EV.ENABLE,
                        .fflags = std.posix.system.NOTE.CRITICAL,
                        .data = 0,
                        .udata = 0,
                    };
                    var out: [1]std.posix.Kevent = undefined;
                    _ = try std.posix.kevent(kq, &.{event}, &out, &.{ .sec = 2, .nsec = 0 });
                },
                else => {},
            }

            if (!self.running.load(.acquire)) {
                running = false;
            }

            const rlen = std.posix.recvfrom(sockfd, buf, 0, null, null) catch {
                continue;
            };

            if (rlen == 0) {
                continue;
            }

            const maybe_node = self.b.get();
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
            self.q.put(node);

            // notify the processing thread
            self.packets_signal.emit() catch |err| {
                std.log.err("listener: can't emit signal: {}", .{err});
            };

            const tmp: u64 = @intCast(std.time.milliTimestamp() - last_drop_message);
            if (tmp > 10000) {
                last_drop_message = std.time.milliTimestamp();
                std.log.info("listener drops: {d}/s ({d} last {d}s)", .{ @divTrunc(drops, @divTrunc(tmp, 1000)), drops, 10 });

                // FIXME(remy): just because of this call, the main Sampler isn't thread safe
                // and has to lock its maps. Either the sampler_telemetry should have a thread
                // safe path (with sampleDist and sampleSerie with a lock parameter) or it should
                // not be emitted from the listener thread.
                self.sampler.sampleTelemetry(.Counter, "statsd.listener.packets_drop", @floatFromInt(drops), .empty);

                drops = 0;
            }
        }
    }

    fn open_socket(self: *ListenerThread) !i32 {
        var sockfd: i32 = 0;
        if (self.uds) {
            sockfd = try open_socket_uds();
            std.log.info("starting the listener on statsd.sock", .{});
        } else {
            sockfd = try open_socket_udp();
            std.log.info("starting the listener on localhost:8125", .{});
        }

        return sockfd;
    }

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
};
