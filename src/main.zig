const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const fmt = std.fmt;
const assert = std.debug.assert;

const listener = @import("listener.zig").listener;
const Packet = @import("listener.zig").Packet;

const AtomicQueue = @import("atomic_queue.zig").AtomicQueue;
const metric = @import("metric.zig");
const Config = @import("config.zig").Config;
const Parser = @import("parser.zig").Parser;
const PreallocatedPacketsPool = @import("preallocated_packets_pool.zig").PreallocatedPacketsPool;
const Sampler = @import("sampler.zig").Sampler;
const MeasureAllocator = @import("measure_allocator.zig").MeasureAllocator;

/// flush_frequency represents how often we flush the sampler (in ms).
pub const flush_frequency = 10000;

// TODO(remy): comment me
pub const ThreadContext = struct {
    /// packets read from the network waiting to be processed
    q: AtomicQueue(Packet),
    /// packets buffers available to share data between the listener thread
    /// and the parser thread.
    b: *PreallocatedPacketsPool,
    /// is running in UDS
    uds: bool,
    /// sampler to send health telemetry from the server itself
    sampler: *Sampler,
};

var running: std.atomic.Value(bool) = std.atomic.Value(bool).init(true);

fn catchSigint() !void {
    switch (builtin.os.tag) {
        .linux, .macos => {
            const action = std.posix.Sigaction{
                .handler = .{ .handler = sigintHandler },
                .mask = std.posix.empty_sigset,
                .flags = 0,
            };

            std.posix.sigaction(std.c.SIG.INT, &action, null);
        },
        else => {},
    }
}

fn sigintHandler(_: c_int) callconv(.C) void {
    running.store(false, .monotonic);
    std.log.debug("sending stop signal", .{});
}

pub fn main() !void {
    // queue communicating packets to parse
    const queue = AtomicQueue(Packet).init();

    // gpa
    var gpa = std.heap.DebugAllocator(.{
        .stack_trace_frames = 10,
    }){};
    defer _ = gpa.detectLeaks();

    // catch close signal
    try catchSigint();

    // pre-alloc 256 packets that will be re-used to contain the read data
    // these packets will do round-trips between the listener and the parser.
    var packets_pool = try PreallocatedPacketsPool.init(gpa.allocator(), 256);
    defer packets_pool.deinit();

    // read config
    const config = try Config.read();

    // create the sampler
    // -----------------

    // this sampler will be used by only one thread, it doesn't need
    // to lock on sampling.
    var sampler = try Sampler.init(gpa.allocator(), config);
    defer sampler.deinit();

    // shared context between the threads
    // ---------------------------------

    var tx = ThreadContext{
        .q = queue,
        .b = &packets_pool,
        .uds = config.uds,
        .sampler = &sampler,
    };

    // spawn the listening thread
    const thread = try std.Thread.spawn(std.Thread.SpawnConfig{}, listener, .{&tx});
    thread.detach();

    // prepare the allocator used by the parser
    // TODO(remy): move all of this in the parser object
    // ----------------------------------------

    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();
    var measured_arena = MeasureAllocator.init(arena.allocator());

    var next_flush = std.time.milliTimestamp() + flush_frequency;
    var packets_parsed: u32 = 0;
    var metrics_parsed: u32 = 0;
    var bytes_parsed: u64 = 0;

    // pipeline mainloop
    while (true) {
        while (!tx.q.isEmpty()) {
            if (tx.q.get()) |node| {
                // parse the packets
                if (Parser.parse_packet(measured_arena.allocator(), node.data)) |metrics| {
                    packets_parsed += 1;
                    // sampling
                    for (metrics.items) |m| {
                        metrics_parsed += 1;
                        try sampler.sample(m);
                    }
                } else |err| {
                    std.log.err("can't parse packet: {s}", .{@errorName(err)});
                    std.log.err("packet: {s}", .{node.data.payload});
                }

                bytes_parsed += node.data.len;

                // send this buffer back to the usable queue of buffers
                tx.b.put(node);

                // TODO(remy): this should be a function of the paresr implementation
                if (measured_arena.allocated > config.max_mem_mb * 1024 * 1024) {
                    break;
                }
            }
        }

        if (std.time.milliTimestamp() > next_flush) {
            sampler.flush() catch |err| {
                std.log.err("can't flush: {s}", .{@errorName(err)});
            };

            std.log.info("packets parsed: {d}/s ({d} last {d}s)", .{ @divTrunc(packets_parsed, (flush_frequency / 1000)), packets_parsed, flush_frequency / 1000 });
            std.log.info("metrics parsed: {d}/s ({d} last {d}s)", .{ @divTrunc(metrics_parsed, (flush_frequency / 1000)), metrics_parsed, flush_frequency / 1000 });
            std.log.info("reporting {d} bytes used by the parser", .{measured_arena.allocated});

            sampler.sample_telemetry(.Counter, "statsd.parser.packets_parsed", @floatFromInt(packets_parsed), .empty);
            sampler.sample_telemetry(.Counter, "statsd.parser.metrics_parsed", @floatFromInt(metrics_parsed), .empty);
            sampler.sample_telemetry(.Counter, "statsd.parser.bytes_parsed", @floatFromInt(bytes_parsed), .empty);
            sampler.sample_telemetry(.Counter, "statsd.parser.pool.available_packet", @floatFromInt(packets_pool.items.size()), .empty);
            sampler.sample_telemetry(.Gauge, "statsd.parser.bytes_inuse", @floatFromInt(measured_arena.allocated), .empty);

            packets_parsed = 0;
            metrics_parsed = 0;
            bytes_parsed = 0;

            next_flush = std.time.milliTimestamp() + flush_frequency;
        }

        //        std.posix.nanosleep(0, 100000);

        if (!running.load(.monotonic)) {
            break;
        }

        // TODO(remy): move all of this in the parser implementation
        if (measured_arena.allocated > config.max_mem_mb * 1024 * 1024) {
            // std.log.debug("memory arena has reached {}MB, deinit and recreate.", .{measure_allocator.allocated / 1024 / 1024});
            // free the memory and reset the arena and measure allocator.
            _ = arena.reset(.free_all);
            measured_arena.parent_allocator = arena.allocator();
            measured_arena.allocated = 0;
        }
    }

    try std.fs.cwd().deleteFile("statsd.sock");
}
