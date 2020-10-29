const std = @import("std");
const c = @cImport(@cInclude("curl/curl.h"));

const Sample = @import("sampler.zig").Sample;
const Config = @import("config.zig").Config;
const metric = @import("metric.zig");

//const endpoint = "https://agent.datadoghq.com/api/v1/series";
const endpoint = "http://localhost:8001";

pub const Forwarder = struct {
    /// flush is responsible for sending all the given metrics to some HTTP route.
    /// It owns the list of metrics and is responsible for freeing its memory.
    pub fn flush(allocator: *std.mem.Allocator, config: Config, samples: *std.AutoHashMap(u64, Sample)) !void {
        var buf = try std.ArrayListSentineled(u8, 0).initSize(allocator, 0);
        defer buf.deinit();
        defer samples.*.deinit();

        try buf.appendSlice("{\"series\":[");

        // append every sample

        var first: bool = true;
        var iterator = samples.*.iterator();
        var kv = iterator.next();
        while (kv != null) {
            if (!first) {
                try buf.append(',');
            }
            first = false;
            try write_sample(allocator, config, &buf, kv.?.value);
            kv = iterator.next();
        }

        try buf.appendSlice("]}");

        try send_http_request(allocator, config, buf);
    }

    fn write_sample(allocator: *std.mem.Allocator, config: Config, buf: *std.ArrayListSentineled(u8, 0), sample: Sample) !void {
        // {
        //   "metric": "system.mem.used",
        //   "points": [
        //     [
        //       1589122593,
        //       1811.29296875
        //     ]
        //   ],
        //   "tags": [],
        //   "host": "hostname",
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
            "{{\"metric\":\"{}\",\"host\":\"{}\",\"type\":\"{}\",\"points\":[[{},{d}]],\"interval\":0}}",
            .{
                sample.metric_name,
                config.hostname,
                t,
                @divTrunc(std.time.milliTimestamp(), 1000),
                sample.value,
            },
        );
        defer allocator.free(json);

        // append it to the main buffer
        try buf.*.appendSlice(json);
    }

    fn send_http_request(allocator: *std.mem.Allocator, config: Config, buf: std.ArrayListSentineled(u8, 0)) !void {
        _ = c.curl_global_init(c.CURL_GLOBAL_ALL);

        var curl: ?*c.CURL = null;
        var res: c.CURLcode = undefined;
        var headers: [*c]c.curl_slist = null;

        // url
        var url = try std.fmt.allocPrint0(allocator, "{}?api_key={}", .{ endpoint, config.apikey });
        defer allocator.free(url);

        // apikeyHeader
        var apikeyHeader = try std.fmt.allocPrint0(allocator, "Dd-Api-Key: {}", .{config.apikey});
        defer allocator.free(apikeyHeader);

        curl = c.curl_easy_init();
        if (curl != null) {
            // url
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_URL, @ptrCast([*:0]const u8, url));

            // body
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POSTFIELDSIZE, buf.len());
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POST, @as(c_int, 1));
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POSTFIELDS, @ptrCast([*:0]const u8, buf.span()));

            // http headers
            headers = c.curl_slist_append(headers, "Content-Type: application/json");
            headers = c.curl_slist_append(headers, @ptrCast([*:0]const u8, apikeyHeader));
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_HTTPHEADER, headers);

            // perform the call
            res = c.curl_easy_perform(curl);
            if (@enumToInt(res) != @bitCast(c_uint, c.CURLE_OK)) {
                _ = c.printf("curl_easy_perform() failed: %s\n", c.curl_easy_strerror(res));
            }

            c.curl_slist_free_all(headers);
            c.curl_easy_cleanup(curl);
        }

        c.curl_global_cleanup();
        std.debug.warn("http flush done, request payload size: {}\n", .{buf.len()});
    }
};
