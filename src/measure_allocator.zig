const std = @import("std");
const Allocator = std.mem.Allocator;
const warn = std.debug.warn;
const assert = @import("std").debug.assert;

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

        fn alloc(allocator: *Allocator, len: usize, ptr_align: u29, len_align: u29, ret_len: usize) std.mem.Allocator.Error![]u8 {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            const result = self.parent_allocator.allocFn(self.parent_allocator, len, ptr_align, len_align, ret_len);
            if (result) |buff| {
                self.allocated += len;
            } else |err| {
                return err;
            }
            return result;
        }

        fn resize(allocator: *Allocator, buf: []u8, buf_align: u29, new_len: usize, len_align: u29, ret_len: usize) std.mem.Allocator.Error!usize {
            const self = @fieldParentPtr(Self, "allocator", allocator);
            var old_len: usize = buf.len;
            const result = self.parent_allocator.resizeFn(self.parent_allocator, buf, buf_align, new_len, len_align, ret_len);
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

test "measure allocator counter and memory leak" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    var measure_allocator = MeasureAllocator().init(&arena.allocator);

    var loop_count: u32 = 0;
    var alloc_count: u32 = 0;
    var reset_count: u32 = 0;
    while (loop_count < 1024) {
        var data = try measure_allocator.allocator.alloc(u8, 8192);
        measure_allocator.allocator.free(data); // is actually not freeing anything
        assert(measure_allocator.allocated == (8192 * (alloc_count + 1)));
        alloc_count += 1;
        loop_count += 1;

        if (measure_allocator.allocated > 15000) {
            arena.deinit();
            arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            measure_allocator = MeasureAllocator().init(&arena.allocator);
            reset_count += 1;
            alloc_count = 0;
        }
    }

    assert(reset_count == 1024 / 2); // it should reset every 2 runs

    arena.deinit(); // is freeing all the data
}
