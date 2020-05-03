const std = @import("std");

const Sample = @import("sampler.zig").Sample;

pub const Forwarder = struct {
    /// flush is responsible for sending all the given metrics to some HTTP route.
    /// It owns the list of metrics and is responsible for freeing its memory.
    pub fn flush(samples: std.AutoHashMap(u64, Sample)) !void {
        _ = try std.json.stringify("hello", std.json.StringifyOptions{}, std.io.getStdOut().outStream());
        samples.deinit();
    }

    fn writeSample(sample: Sample) []u8 {
    }
};
