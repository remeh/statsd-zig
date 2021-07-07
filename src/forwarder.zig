const std = @import("std");
const c = @cImport(@cInclude("curl/curl.h"));

const Sample = @import("sampler.zig").Sample;
const Config = @import("config.zig").Config;
const metric = @import("metric.zig");

//const endpoint = "https://agent.datadoghq.com/api/v1/series";
const endpoint = "http://localhost:8001";

const max_retry_per_transaction = 5;
// TODO(remy): instead of limiting on the amount of transactions, we could limit
// on the RAM usage of these stored transactions, i.e. max_stored_transactions_ram_usage
// or something like this.
const max_stored_transactions = 10;

pub const ForwarderError = error{RequestFailed};

/// Transaction is the content of the HTTP request sent to Datadog.
/// If sending a transaction to the intake fails, we'll keep it in RAM for some
/// time to do some retries.
pub const Transaction = struct {
    allocator: *std.mem.Allocator,
    data: std.ArrayList(u8),
    creation_time: i64, // unit: ms
    tries: u8,

    pub fn init(allocator: *std.mem.Allocator, creation_time: i64) !*Transaction {
        var rv = try allocator.create(Transaction);
        rv.allocator = allocator;
        rv.data = std.ArrayList(u8).init(allocator);
        rv.creation_time = creation_time;
        rv.tries = 0;
        return rv;
    }

    pub fn deinit(self: *Transaction) void {
        self.data.deinit();
        self.allocator.destroy(self);
    }
};

pub const Forwarder = struct {
    transactions: std.ArrayList(*Transaction),

    pub fn deinit(self: *Forwarder) void {
        for (self.transactions.items) |tx| {
            tx.deinit();
        }
        self.transactions.deinit();
    }

    /// flush is responsible for sending all the given metrics to some HTTP route.
    /// It owns the list of metrics and is responsible for freeing its memory.
    pub fn flush(self: *Forwarder, allocator: *std.mem.Allocator, config: Config, samples: *std.AutoHashMap(u64, Sample)) !void {
        defer std.debug.warn("transactions stored in RAM: {}\n", .{self.transactions.items.len});

        // try to send a new transaction only if there is metrics to send
        // ---
        if (samples.count() > 0) {
            const tx = try create_transaction(allocator, config, samples, std.time.milliTimestamp());

            // try to send the transaction
            send_http_request(allocator, config, tx) catch |err| {
                std.debug.warn("can't send a transaction: {s}\nstoring the transaction of size {d} bytes\n", .{ err, tx.data.items.len });

                // limit the amount of transactions stored
                if (self.transactions.items.len > max_stored_transactions) {
                    std.debug.warn("too many transactions stored, removing a random old one.\n", .{});
                    var droppedtx = self.transactions.orderedRemove(0);
                    droppedtx.deinit();
                }

                self.transactions.append(tx) catch |err2| {
                    std.debug.warn("can't store the failing transaction {s}\n", .{err2});
                    tx.deinit();
                };
                return;
            };

            // this transaction succeed, we can deinit it.
            tx.deinit();
        }

        // try to replay some older transactions.
        // ---

        if (self.transactions.items.len > 0) {
            // don't replay more than 3 transactions
            self.replay_old_transactions(allocator, config, 3);
        }
    }

    /// creates a transaction with the given metric samples.
    fn create_transaction(allocator: *std.mem.Allocator, config: Config, samples: *std.AutoHashMap(u64, Sample), creation_time: i64) !*Transaction {
        var tx = try Transaction.init(allocator, creation_time);

        try tx.data.appendSlice("{\"series\":[");

        // append every sample

        var first: bool = true;
        var iterator = samples.*.iterator();
        while (iterator.next()) |kv| {
            if (!first) {
                try tx.data.append(',');
            }
            first = false;
            try write_sample(allocator, config, tx, kv.value_ptr.*);
        }

        try tx.data.appendSlice("]}");
        return tx;
    }

    fn replay_old_transactions(self: *Forwarder, allocator: *std.mem.Allocator, config: Config, count: usize) void {
        var i: usize = 0;
        while (i < 3) : (i += 1) {
            if (self.transactions.items.len == 0) {
                break;
            }
            var tx = self.transactions.orderedRemove(0);
            send_http_request(allocator, config, tx) catch |err| {
                std.debug.warn("error while retrying a transaction: {s}\n", .{err});
                if (tx.tries < max_retry_per_transaction) {
                    tx.tries += 1;
                    self.transactions.append(tx) catch |err2| {
                        tx.deinit();
                        std.debug.warn("can't store the failing transaction {s}\n", .{err2});
                    };
                } else {
                    tx.deinit();
                    std.debug.warn("this transaction of {d} bytes dating from {d} has been dropped.\n", .{ tx.data.items.len, tx.creation_time });
                }
                break; // useless to try more transaction right now
            };
        }
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
            "{{\"metric\":\"{s}\",\"host\":\"{s}\",\"type\":\"{s}\",\"points\":[[{d},{d}]],\"interval\":0}}",
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
        var url = try std.fmt.allocPrint0(allocator, "{s}?api_key={s}", .{ endpoint, config.apikey });
        defer allocator.free(url);

        // apikeyHeader
        var apikeyHeader = try std.fmt.allocPrint0(allocator, "Dd-Api-Key: {s}", .{config.apikey});
        defer allocator.free(apikeyHeader);

        curl = c.curl_easy_init();
        if (curl != null) {
            // url
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_URL, @ptrCast([*:0]const u8, url));

            // body
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POSTFIELDSIZE, tx.data.items.len);
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POST, @as(c_int, 1));
            _ = c.curl_easy_setopt(curl, c.CURLoption.CURLOPT_POSTFIELDS, @ptrCast([*:0]const u8, tx.data.items));

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
            std.debug.warn("http flush done, request payload size: {}\n", .{tx.data.items.len});
        }
    }
};

test "transaction_mem_usage" {
    const allocator = std.testing.allocator;
    var name = try allocator.alloc(u8, "my.metric".len);
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

    var samples = std.AutoHashMap(u64, Sample).init(allocator);
    try samples.put(123456789, sample);

    var tx = try Forwarder.create_transaction(allocator, config, &samples, 0);
    tx.deinit();

    samples.deinit();
    allocator.free(name);
}

// TODO(remy): add a test for replay_old_transactions

test "write_sample_test" {
    const allocator = std.testing.allocator;
    var tx = try Transaction.init(allocator, 0);

    var name = try allocator.alloc(u8, "my.metric".len);
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

    try Forwarder.write_sample(allocator, config, tx, sample);
    tx.deinit();

    allocator.free(name);
}
