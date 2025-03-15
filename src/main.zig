const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const fmt = std.fmt;
const assert = std.debug.assert;

const AtomicQueue = @import("atomic_queue.zig").AtomicQueue;
const event = @import("event.zig");
const Config = @import("config.zig").Config;
const Listener = @import("listener.zig").Listener;
const MeasureAllocator = @import("measure_allocator.zig").MeasureAllocator;
const Parser = @import("parser.zig").Parser;
const Packet = @import("listener.zig").Packet;
const Sampler = @import("sampler.zig").Sampler;
const Signal = @import("signal.zig").Signal;

/// flush_frequency represents how often we flush the sampler (in ms).
pub const flush_frequency_ms = 10000;

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
    // gpa
    var gpa = std.heap.DebugAllocator(.{
        .stack_trace_frames = 10,
    }){};
    defer _ = gpa.detectLeaks();

    // catch close signal
    try catchSigint();

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

    // creates the listener and spawn the listening thread
    var listener = try Listener.init(gpa.allocator(), &sampler, .{
        .packets_pool_size = 4096,
        .uds = config.uds,
    });
    defer listener.deinit();

    // prepare the allocator used by the parser
    // TODO(remy): move all this initialisation in the parser implementation
    // ----------------------------------------

    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();
    var measured_arena = MeasureAllocator.init(arena.allocator());

    var next_flush = std.time.milliTimestamp() + flush_frequency_ms;
    var packets_parsed: u32 = 0;
    var events_parsed: u32 = 0;
    var bytes_parsed: u64 = 0;

    // pipeline mainloop
    while (true) {
        // wait for a signal from the listener, or until 3000ms have elapsed.
        listener.thread.packets_signal.wait(3000) catch |err| {
            std.log.debug("main: on signal wait: {}", .{err});
        };

        // is there something to process in the listener?
        while (!listener.thread.q.isEmpty()) {
            if (listener.thread.q.get()) |node| {
                // parse the packets
                if (Parser.parse_packet(measured_arena.allocator(), node.data)) |events| {
                    packets_parsed += 1;
                    // sampling
                    for (events.items) |m| {
                        events_parsed += 1;
                        try sampler.sample(m);
                    }
                } else |err| {
                    std.log.err("can't parse packet: {s}", .{@errorName(err)});
                    std.log.err("packet: {s}", .{node.data.payload});
                }

                bytes_parsed += node.data.len;

                // send this buffer back to the usable queue of buffers for the listener
                listener.thread.b.put(node);

                // TODO(remy): this should be moved in the parser implementation
                if (measured_arena.allocated > config.max_mem_mb * 1024 * 1024) {
                    break;
                }
            }
        }

        if (std.time.milliTimestamp() > next_flush) {
            sampler.flush() catch |err| {
                std.log.err("can't flush: {s}", .{@errorName(err)});
            };

            std.log.info("packets parsed: {d}/s ({d} last {d}s)", .{ @divTrunc(packets_parsed, (flush_frequency_ms / 1000)), packets_parsed, flush_frequency_ms / 1000 });
            std.log.info("events parsed: {d}/s ({d} last {d}s)", .{ @divTrunc(events_parsed, (flush_frequency_ms / 1000)), events_parsed, flush_frequency_ms / 1000 });
            std.log.info("reporting {d} bytes used by the parser", .{measured_arena.allocated});

            sampler.sampleTelemetry(.Counter, "statsd.parser.packets_parsed", @floatFromInt(packets_parsed), .empty);
            sampler.sampleTelemetry(.Counter, "statsd.parser.events_parsed", @floatFromInt(events_parsed), .empty);
            sampler.sampleTelemetry(.Counter, "statsd.parser.bytes_parsed", @floatFromInt(bytes_parsed), .empty);
            sampler.sampleTelemetry(.Counter, "statsd.parser.pool.available_packet", @floatFromInt(listener.packets_pool.items.size()), .empty);
            sampler.sampleTelemetry(.Gauge, "statsd.parser.bytes_inuse", @floatFromInt(measured_arena.allocated), .empty);

            packets_parsed = 0;
            events_parsed = 0;
            bytes_parsed = 0;

            next_flush = next_flush + flush_frequency_ms;
        }

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

    if (config.uds) {
        try std.fs.cwd().deleteFile("statsd.sock");
    }
}
