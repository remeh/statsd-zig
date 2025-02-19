const std = @import("std");

const AtomicQueue = @import("atomic_queue.zig").AtomicQueue;
const Packet = @import("listener.zig").Packet;

// TODO(remy): comment me
// TODO(remy): unit test
pub const PreallocatedPacketsPool = struct {
    allocator: std.mem.Allocator,
    items: AtomicQueue(Packet),

    /// init prepares a preallocated packets pool.
    pub fn init(allocator: std.mem.Allocator, size: usize) !*PreallocatedPacketsPool {
        var rv = try allocator.create(PreallocatedPacketsPool);
        rv.allocator = allocator;
        rv.items = AtomicQueue(Packet).init();

        std.log.debug("allocating the preallocated packets pool, will use: {d}MB", .{@sizeOf(Packet) * size / 1024 / 1024});

        var i: usize = 0;
        while (i < size) : (i += 1) {
            const packet_node = try allocator.create(AtomicQueue(Packet).Node);
            packet_node.data = Packet{
                .payload = std.mem.zeroes([8192]u8),
                .len = 0,
            };
            rv.put(packet_node);
        }

        return rv;
    }

    pub fn deinit(self: *PreallocatedPacketsPool) void {
        while (self.get()) |node| {
            self.allocator.destroy(node);
        }
        self.allocator.destroy(self);
    }

    pub fn get(self: *PreallocatedPacketsPool) ?*AtomicQueue(Packet).Node {
        return self.items.get();
    }

    pub fn put(self: *PreallocatedPacketsPool, node: *AtomicQueue(Packet).Node) void {
        self.items.put(node);
    }

    pub fn isEmpty(self: *PreallocatedPacketsPool) bool {
        return self.items.isEmpty();
    }
};
