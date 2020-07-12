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
                    .allocFn = alloc,
                    .resizeFn = resize,
                },
                .parent_allocator = parent_allocator,
                .allocated = 0,
            };
        }

        fn alloc(allocator: *Allocator, len: usize, ptr_align: u29, len_align: u29) std.mem.Allocator.Error![]u8 {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            const result = self.parent_allocator.allocFn(self.parent_allocator, len, ptr_align, len_align);
            if (result) |buff| {
                self.allocated += len;
            } else |err| {
                return err;
            }
            return result;
        }

        fn resize(allocator: *Allocator, buf: []u8, new_len: usize, len_align: u29) std.mem.Allocator.Error!usize {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            var old_len: usize = buf.len;
            const result = self.parent_allocator.resizeFn(self.parent_allocator, buf, new_len, len_align);
            if (result) |buff| {
                if (old_len < new_len) {
                    self.allocated += new_len - old_len;
                }
            } else |err| {
                return err;
            }
            return result;
        }
    };
}
