const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = @import("std").debug.assert;

/// MeasureAllocator is counting how much bytes were succesfully allocated with
/// the parent_allocator.
pub fn MeasureAllocator() type {
    return struct {
        parent_allocator: Allocator,
        allocated: u64,

        const Self = @This();

        pub fn init(parent_allocator: Allocator) Self {
            return .{
                .parent_allocator = parent_allocator,
                .allocated = 0,
            };
        }

        pub fn allocator(self: *Self) Allocator {
            return .{
                .ptr = self,
                .vtable = &.{
                    .alloc = alloc,
                    .resize = resize,
                    .free = free,
                },
            };
        }

        fn alloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_len: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const result = self.parent_allocator.rawAlloc(len, ptr_align, ret_len);
            if (result != null) {
                self.allocated += len;
            }
            return result;
        }

        fn resize(ctx: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_len: usize) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            var old_len: usize = buf.len;
            const result = self.parent_allocator.rawResize(buf, buf_align, new_len, ret_len);
            if (result) {
                if (old_len < new_len) {
                    self.allocated += new_len - old_len;
                }
            }
            return result;
        }

        fn free(ctx: *anyopaque, buf: []u8, buf_align: u8, ra: usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.parent_allocator.rawFree(buf, buf_align, ra);
            self.allocated -= buf.len;
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
