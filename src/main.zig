const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const assert = std.debug.assert;
const Queue = std.atomic.Queue;

const listener = @import("listener.zig").listener;
const Packet = @import("listener.zig").Packet;

const Metric = @import("metric.zig").Metric;
const Config = @import("config.zig").Config;
const Parser = @import("parser.zig").Parser;
const Sampler = @import("sampler.zig").Sampler;
const MeasureAllocator = @import("measure_allocator.zig").MeasureAllocator;

/// flush_frequency represents how often we flush the sampler (in ms).
pub const flush_frequency = 15000;

pub const ThreadContext = struct {
    // packets read from the network waiting to be processed
    q: std.atomic.Queue(Packet),
    // packets buffers available to share data between the listener thread
    // and the parser thread.
    b: std.atomic.Queue(Packet),
    // is running in UDS
    uds: bool,
};

pub fn main() !void {
    // queue communicating packets to parse
    var queue = Queue(Packet).init();

    // pre-alloc 4096 packets that will be re-used to contain the read data
    // these packets will do round-trips between the listener and the parser.
    var packet_buffers = Queue(Packet).init();
    var i: usize = 0;

    // TODO(remy): add a knob here
    while (i < 4096) {
        var packet_node: *Queue(Packet).Node = try std.heap.page_allocator.create(Queue(Packet).Node);
        packet_node.data = Packet{
            .payload = try std.heap.page_allocator.alloc(u8, 8192),
            .len = 0,
        };
        packet_buffers.put(packet_node);
        i += 1;
    }

    // read config
    var config = try Config.read();

    // shared context
    var tx = ThreadContext{
        .q = queue,
        .b = packet_buffers,
        .uds = config.uds,
    };

    // create the sampler
    var sampler = try Sampler.init(std.heap.page_allocator);

    // spawn the listening thread
    const thread = try std.Thread.spawn(std.Thread.SpawnConfig{}, listener, .{&tx});
    thread.detach();

    // prepare the allocator used by the parser
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var measure_allocator = MeasureAllocator().init(arena.allocator());

    var next_flush = std.time.milliTimestamp() + flush_frequency;
    var packets_parsed: u32 = 0;
    var metrics_parsed: u32 = 0;

    // pipeline mainloop
    // TODO(remy): listen for Ctrl-C to clean up before exiting
    while (true) {
        while (!tx.q.isEmpty()) {
            var node = tx.q.get().?;
            // parse the packets
            if (Parser.parse_packet(measure_allocator.allocator(), node.data)) |metrics| {
                packets_parsed += 1;
                // sampling
                i = 0;
                while (i < metrics.items.len) {
                    metrics_parsed += 1;
                    try sampler.sample(metrics.items[i]);
                    i += 1;
                }
            } else |err| {
                std.log.err("can't parse packet: {s}", .{@errorName(err)});
                std.log.err("packet: {s}", .{node.data.payload});
            }

            // send this buffer back to the usable queue of buffers
            tx.b.put(node);

            if (measure_allocator.allocated > config.max_mem_mb * 1024 * 1024) {
                break;
            }
        }

        if (measure_allocator.allocated > config.max_mem_mb * 1024 * 1024) {
            std.log.debug("memory arena has reached {}MB, deinit and recreate.", .{measure_allocator.allocated / 1024 / 1024});
            // free the memory and reset the arena and measure allocator.
            arena.deinit();
            arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            measure_allocator = MeasureAllocator().init(arena.allocator());
        }

        if (std.time.milliTimestamp() > next_flush) {
            sampler.flush(config) catch |err| {
                std.log.err("can't flush: {s}", .{@errorName(err)});
            };

            std.log.info("packets parsed: {d}/s ({d} last {d}s)", .{ @divTrunc(packets_parsed, (flush_frequency / 1000)), packets_parsed, flush_frequency / 1000 });
            std.log.info("metrics parsed: {d}/s ({d} last {d}s)", .{ @divTrunc(metrics_parsed, (flush_frequency / 1000)), metrics_parsed, flush_frequency / 1000 });
            packets_parsed = 0;
            metrics_parsed = 0;

            next_flush = std.time.milliTimestamp() + flush_frequency;
        }
    }

    // TODO(remy): delete the socket
}
