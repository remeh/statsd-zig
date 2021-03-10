const std = @import("std");
const c = @cImport(@cInclude("curl/curl.h"));

const Sample = @import("sampler.zig").Sample;
const Config = @import("config.zig").Config;
const metric = @import("metric.zig");

//const endpoint = "https://agent.datadoghq.com/api/v1/series";
const endpoint = "http://localhost:8001";

pub const ForwarderError = error{RequestFailed};

/// Transaction is the content of the HTTP request sent to Datadog.
/// If sending a transaction to the intake fails, we'll keep it in RAM for some
/// time to do some retries.
pub const Transaction = struct {
    data: std.ArrayListSentineled(u8, 0),
    creation_time: i64,

    pub fn deinit(self: *Transaction) void {
        self.data.deinit();
    }
};

pub const Forwarder = struct {
    transactions: std.ArrayList(*Transaction),

    pub fn deinit(self: *Forwarder) void {
        self.transactions.deinit();
    }

    /// flush is responsible for sending all the given metrics to some HTTP route.
    /// It owns the list of metrics and is responsible for freeing its memory.
    pub fn flush(self: *Forwarder, allocator: *std.mem.Allocator, config: Config, samples: *std.AutoHashMap(u64, Sample)) !void {
        // TODO(remy): we should not create a new transaction if there is no metrics
        // to flush, however, we should still try to send old transactions if any
        var tx = &Transaction{
            .data = try std.ArrayListSentineled(u8, 0).initSize(allocator, 0),
            .creation_time = 0,
        };

        try tx.data.appendSlice("{\"series\":[");

        // append every sample

        var first: bool = true;
        var iterator = samples.*.iterator();
        var kv = iterator.next();
        while (kv != null) {
            if (!first) {
                try tx.data.append(',');
            }
            first = false;
            try write_sample(allocator, config, tx, kv.?.value);
            kv = iterator.next();
        }

        try tx.data.appendSlice("]}");

        send_http_request(allocator, config, tx) catch |err| {
            std.debug.warn("can't send a transaction: {}\nstoring the transaction of size {} bytes\n", .{ err, tx.data.len() });
            // TODO(remy): we want to limit how many transactions are stored in RAM
            self.transactions.append(tx) catch |err2| {
                std.debug.warn("can't store the failing transaction {}\n", .{err2});
                tx.deinit();
            };
            return;
        };

        // TODO(remy): we should see if there is some other transaction to
        // retry.

        // this transaction succeed, we can deinit it.
        tx.deinit();
    }

    fn write_sample(allocator: *std.mem.Allocator, config: Config, tx: *Transaction, sample: Sample) !void {
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
        try tx.*.data.appendSlice(json);
    }

    fn send_http_request(allocator: *std.mem.Allocator, config: Config, tx: *Transaction) !void {
        _ = c.curl_global_init(c.CURL_GLOBAL_ALL);

        var failed: bool = false;
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
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POSTFIELDSIZE, tx.data.len());
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POST, @as(c_int, 1));
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POSTFIELDS, @ptrCast([*:0]const u8, tx.data.span()));

            // http headers
            headers = c.curl_slist_append(headers, "Content-Type: application/json");
            headers = c.curl_slist_append(headers, @ptrCast([*:0]const u8, apikeyHeader));
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_HTTPHEADER, headers);

            // perform the call
            res = c.curl_easy_perform(curl);
            if (@enumToInt(res) != @bitCast(c_uint, c.CURLE_OK)) {
                _ = c.printf("curl_easy_perform() failed: %s\n", c.curl_easy_strerror(res));
                failed = true;
            }

            c.curl_slist_free_all(headers);
            c.curl_easy_cleanup(curl);
        }

        c.curl_global_cleanup();

        if (failed) {
            return ForwarderError.RequestFailed;
        } else {
            std.debug.warn("http flush done, request payload size: {}\n", .{tx.data.len()});
        }
    }
};

// TODO(remy): write some tests around the transaction

test "write_sample_test" {
    var buf = try std.ArrayListSentineled(u8, 0).initSize(std.testing.allocator, 0);

    var name = try std.testing.allocator.alloc(u8, "my.metric".len);
    std.mem.copy(u8, name, "my.metric");

    var sample = Sample{
        .metric_name = name,
        .metric_type = metric.MetricTypeGauge,
        .samples = 1,
        .value = 1,
    };

    var config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .max_mem_mb = 20000,
    };

    try Forwarder.write_sample(std.testing.allocator, config, &buf, sample);

    std.testing.allocator.free(name);
    buf.deinit();
}
