test "all tests" {
    _ = @import("forwarder.zig");
    _ = @import("listener.zig");
    _ = @import("measure_allocator.zig");
    _ = @import("parser.zig");
    _ = @import("sampler.zig");
    _ = @import("ddsketch.zig");
    _ = @import("protobuf.zig");
}
