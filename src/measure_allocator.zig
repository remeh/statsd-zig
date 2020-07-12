const std = @import("std");
const Allocator = std.mem.Allocator;

/// MeasureAllocator is counting how much bytes were succesfully allocated with
/// the parent_allocator.
pub fn MeasureAllocator() type {
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
                .allocated = 0,
            };
        }

        fn realloc(allocator: *Allocator, old_mem: []u8, old_align: u29, new_size: usize, new_align: u29) ![]u8 {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            const result = self.parent_allocator.reallocFn(self.parent_allocator, old_mem, old_align, new_size, new_align);
            if (result) |buff| {
                if (old_mem.len == 0) {
                    self.allocated += new_size - old_mem.len;
                } else {
                    self.allocated += new_size;
                }
            } else |err| {
                return err;
            }
            return result;
        }

        fn shrink(allocator: *Allocator, old_mem: []u8, old_align: u29, new_size: usize, new_align: u29) []u8 {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            const result = self.parent_allocator.shrinkFn(self.parent_allocator, old_mem, old_align, new_size, new_align);
            return result;
        }
    };
}
