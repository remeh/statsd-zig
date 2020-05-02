const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn LogAllocator() type {
    return struct {
        allocator: Allocator,
        parent_allocator: *Allocator,
        allocated: u64,

        const Self = @This();

        pub fn init(parent_allocator: *Allocator) Self {
            return Self{
                .allocator = Allocator{
                    .reallocFn = realloc,
                    .shrinkFn = shrink,
                },
                .parent_allocator = parent_allocator,
            };
        }

        fn realloc(allocator: *Allocator, old_mem: []u8, old_align: u29, new_size: usize, new_align: u29) ![]u8 {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            if (old_mem.len == 0) {
                std.debug.warn("allocation of {} ", .{new_size});
            } else {
                std.debug.warn("resize from {} to {} ", .{ old_mem.len, new_size });
            }
            const result = self.parent_allocator.reallocFn(self.parent_allocator, old_mem, old_align, new_size, new_align);
            if (result) |buff| {
                std.debug.warn("success!\n", .{});
            } else |err| {
                std.debug.warn("failure!\n", .{});
            }
            return result;
        }

        fn shrink(allocator: *Allocator, old_mem: []u8, old_align: u29, new_size: usize, new_align: u29) []u8 {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            const result = self.parent_allocator.shrinkFn(self.parent_allocator, old_mem, old_align, new_size, new_align);
            if (new_size == 0) {
                std.debug.warn("free of {} bytes success!\n", .{old_mem.len});
            } else {
                std.debug.warn("shrink from {} bytes to {} bytes success!\n", .{ old_mem.len, new_size });
            }
            return result;
        }
    };
}

