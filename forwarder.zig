const std = @import("std");

const Sample = @import("sampler.zig").Sample;
const metric = @import("metric.zig");

pub const Forwarder = struct {
    /// flush is responsible for sending all the given metrics to some HTTP route.
    /// It owns the list of metrics and is responsible for freeing its memory.
    pub fn flush(allocator: *std.mem.Allocator, samples: std.AutoHashMap(u64, Sample)) !void {
        var response = try std.ArrayListSentineled(u8, 0).initSize(allocator, 0);
        defer response.deinit();
        defer samples.deinit();

        try response.resize(11);
        try response.appendSlice("{\"series\":[");

        // append every sample

        var first: bool = true;
        var iterator = samples.iterator();
        var kv = iterator.next();
        while (kv != null) {
            if (!first) {
                try response.resize(response.len() + 1);
                try response.append(',');
            }
            first = false;
            try write_sample(allocator, &response, kv.?.value);
            kv = iterator.next();
        }

        try response.resize(response.len() + 2);
        try response.appendSlice("]}");
        std.debug.warn("{}", .{response.span()});
    }

    fn write_sample(allocator: *std.mem.Allocator, buf: *std.ArrayListSentineled(u8, 0), sample: Sample) !void {
        // {
        //   "metric": "system.mem.used",
        //   "points": [
        //     [
        //       1589122593,
        //       1811.29296875
        //     ]
        //   ],
        //   "tags": [],
        //   "host": "hooch",
        //   "type": "gauge",
        //   "interval": 0,
        //   "source_type_name": "System"
        // }

        // metric type string
        const t: []u8 = try allocator.alloc(u8, 5);
        defer allocator.free(t);
        switch (sample.metric_type) {
            metric.MetricTypeGauge => {
                std.mem.copy(u8, t, "gauge");
            },
            else => {
                std.mem.copy(u8, t, "count");
            },
        }

        // build the json
        const json = try std.fmt.allocPrint(
            allocator,
            "{{\"metric\":\"{}\",\"host\":\"hooch\",\"type\":\"{}\",\"points\":[[{},{d}]],\"interval\":0}}",
            .{
                sample.metric_name,
                t,
                std.time.milliTimestamp() / 1000,
                sample.value,
            },
        );
        defer allocator.free(json);

        // append it to the main buffer
        try buf.*.appendSlice(json);
    }
};
