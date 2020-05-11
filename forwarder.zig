const std = @import("std");
const c = @cImport(@cInclude("curl/curl.h"));

const Sample = @import("sampler.zig").Sample;
const metric = @import("metric.zig");

pub const Forwarder = struct {
    /// flush is responsible for sending all the given metrics to some HTTP route.
    /// It owns the list of metrics and is responsible for freeing its memory.
    pub fn flush(allocator: *std.mem.Allocator, samples: std.AutoHashMap(u64, Sample)) !void {
        var buf = try std.ArrayListSentineled(u8, 0).initSize(allocator, 0);
        defer buf.deinit();
        defer samples.deinit();

        try buf.resize(11);
        try buf.appendSlice("{\"series\":[");

        // append every sample

        var first: bool = true;
        var iterator = samples.iterator();
        var kv = iterator.next();
        while (kv != null) {
            if (!first) {
                try buf.resize(buf.len() + 1);
                try buf.append(',');
            }
            first = false;
            try write_sample(allocator, &buf, kv.?.value);
            kv = iterator.next();
        }

        try buf.resize(buf.len() + 2);
        try buf.appendSlice("]}");

        send_http_request(&buf);
        std.debug.warn("{}", .{buf.span()});
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

    fn send_http_request(buf: *std.ArrayListSentineled(u8, 0)) void {
        _ = c.curl_global_init(c.CURL_GLOBAL_ALL);

        var curl: ?*c.CURL = undefined;
        var res: c.CURLcode = undefined;
        var headers: [*c]c.curl_slist = null;

        curl = c.curl_easy_init();
        if (curl != null) {
            _ = c.curl_easy_setopt(curl, @intToEnum(c.CURLoption, c.CURLOPT_URL), "http://localhost:8080/?apikey=hello");
            _ = c.curl_easy_setopt(curl, @intToEnum(c.CURLoption, c.CURLOPT_POSTFIELDS), @ptrCast([*c]u8, buf.*.span()));
            _ = c.curl_slist_append(headers, "Content-Type: application/json");
            _ = c.curl_slist_append(headers, "Dd-Api-Key: hello");
            _ = c.curl_easy_setopt(curl, @intToEnum(c.CURLoption, c.CURLOPT_HTTPHEADER), headers);

            res = c.curl_easy_perform(curl);
            if (@enumToInt(res) != @bitCast(c_uint, c.CURLE_OK)) {
                _ = c.fprintf(c.stderr, "curl_easy_perform() failed: %s\n", c.curl_easy_strerror(res));
            }

            c.curl_easy_cleanup(curl);
        }

        c.curl_global_cleanup();
        std.debug.warn("http call done", .{});
    }
};
