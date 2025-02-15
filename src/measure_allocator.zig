const std = @import("std");
const assert = @import("std").debug.assert;

/// MeasureAllocator is counting how much bytes were succesfully allocated with
/// the parent_allocator.
pub const MeasureAllocator = struct {
    parent_allocator: std.mem.Allocator,
    allocated: u64,

    pub fn init(parent_allocator: std.mem.Allocator) MeasureAllocator {
        return MeasureAllocator{
            .parent_allocator = parent_allocator,
            .allocated = 0,
        };
    }

    pub fn allocator(self: *MeasureAllocator) std.mem.Allocator {
        return std.mem.Allocator{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .free = free,
            },
        };
    }

    fn alloc(self: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
        const measure_alloc = @as(*MeasureAllocator, @alignCast(@ptrCast(self)));
        const result = measure_alloc.parent_allocator.rawAlloc(len, ptr_align, ret_addr);
        if (result) |_| {
            measure_alloc.allocated += len;
            //        } else |err| { // FIXME(remy):
            //            return err;
        }
        return result;
    }

    fn resize(self: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
        const measure_alloc = @as(*MeasureAllocator, @alignCast(@ptrCast(self)));
        const old_len: usize = buf.len;
        const result = measure_alloc.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr);
        if (result) {
            if (old_len < new_len) {
                measure_alloc.allocated += new_len - old_len;
            }
        }
        return result;
    }

    fn free(self: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
        const measure_alloc = @as(*MeasureAllocator, @alignCast(@ptrCast(self)));
        measure_alloc.parent_allocator.rawFree(buf, buf_align, ret_addr);
        measure_alloc.allocated -= buf.len;
    }
};

test "measure allocator counter and memory leak" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    var measure_allocator = MeasureAllocator.init(arena.allocator());

    var loop_count: u32 = 0;
    var alloc_count: u32 = 0;
    var reset_count: u32 = 0;
    while (loop_count < 1024) {
        _ = try measure_allocator.allocator().alloc(u8, 8192);
        assert(measure_allocator.allocated == (8192 * (alloc_count + 1)));
        alloc_count += 1;
        loop_count += 1;

        if (measure_allocator.allocated > 15000) {
            arena.deinit();
            arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            measure_allocator = MeasureAllocator.init(arena.allocator());
            reset_count += 1;
            alloc_count = 0;
        }
    }

    assert(reset_count == 1024 / 2); // it should reset every 2 runs
    arena.deinit(); // is freeing all the data
}
