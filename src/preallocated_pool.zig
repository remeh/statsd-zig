const std = @import("std");

const AtomicQueue = @import("atomic_queue.zig").AtomicQueue;

/// PreallocatedPool is a pool of preallocated objects.
/// The pool never blocks: if there is no more object available,
/// the `get` function returns null.
/// The pool itself does not own any object memory, i.e. if the pool
/// `deinit` function is called before all objects have been returned
/// with `put`, it is the caller responsability to free the objects memory.
/// It also means it is correct to `get` an object from the pool and to
/// never return it to the pool, as long as the new owner takes care of
/// deallocating the object memory.
/// Backed by an `AtomicQueue`, `PreallocatedPool` is thread-safe.
pub fn PreallocatedPool(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        items: AtomicQueue(T),

        pub fn init(allocator: std.mem.Allocator, size: usize, zero_value: T) !*PreallocatedPool(T) {
            const rv = try allocator.create(PreallocatedPool(T));
            rv.* = .{
                .allocator = allocator,
                .items = AtomicQueue(T).init(),
            };

            std.log.debug("preallocating the pool of {s}, will use: {d}MB", .{ @typeName(T), @sizeOf(T) * size / 1024 / 1024 });

            var i: usize = 0;
            while (i < size) : (i += 1) {
                const node = try allocator.create(AtomicQueue(T).Node);
                node.data = zero_value;
                rv.put(node);
            }

            return rv;
        }

        pub fn deinit(self: *PreallocatedPool(T)) void {
            while (self.get()) |node| {
                self.allocator.destroy(node);
            }
            self.allocator.destroy(self);
        }

        pub fn get(self: *PreallocatedPool(T)) ?*AtomicQueue(T).Node {
            return self.items.get();
        }

        pub fn put(self: *PreallocatedPool(T), node: *AtomicQueue(T).Node) void {
            self.items.put(node);
        }

        pub fn isEmpty(self: *PreallocatedPool(T)) bool {
            return self.items.isEmpty();
        }
    };
}
