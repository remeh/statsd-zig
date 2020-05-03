const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const assert = std.debug.assert;
const warn = std.debug.warn;
const Queue = std.atomic.Queue;

const listener = @import("listener.zig").listener;
const Packet = @import("listener.zig").Packet;

const Metric = @import("metric.zig").Metric;
const Parser = @import("parser.zig").Parser;
const Sampler = @import("sampler.zig").Sampler;
const MeasureAllocator = @import("measure_allocator.zig").MeasureAllocator;

pub const ThreadContext = struct {
// packets read from the network waiting to be processed
    q: std.atomic.Queue(Packet),
    // packets buffers available to share data between the listener thread
    // and the parser thread.
    b: std.atomic.Queue(Packet)
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

    // shared context
    var tx = ThreadContext{
        .q = queue,
        .b = packet_buffers,
    };

    // create the sampler
    var sampler = try Sampler.init(std.heap.page_allocator);

    // spawn the listening thread
    var listener_thread = std.Thread.spawn(&tx, listener);

    // prepare the allocator used by the parser
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var measure_allocator = MeasureAllocator().init(&arena.allocator);

    // pipeline mainloop
    while (true) {
        while (!tx.q.isEmpty()) {
            var node = tx.q.get().?;
            // parse the packets
            if (Parser.parse_packet(&measure_allocator.allocator, node.data)) |metrics| {

                // sampling
                i = 0;
                while (i < metrics.span().len) {
                    try Sampler.sample(sampler, metrics.span()[i]);
                    i += 1;
                }
            } else |err| {
                warn("can't parse packet: {}\n", .{err});
                warn("packet: {}\n", .{node.data.payload});
            }

            // send this buffer back to the usable queue of buffers
            tx.b.put(node);
        }

        // TODO(remy): add a knob here
        if (measure_allocator.allocated > 256*1024*1024) {
            arena.deinit();
            arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            measure_allocator.allocated = 0;
            Sampler.dump(sampler);
        }
    }
}
