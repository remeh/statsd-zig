const std = @import("std");
const builtin = @import("builtin");

const c = @cImport(@cInclude("curl/curl.h"));

const Distribution = @import("sampler.zig").Distribution;
const Serie = @import("sampler.zig").Serie;
const Config = @import("config.zig").Config;
const metric = @import("metric.zig");
const protobuf = @import("protobuf.zig");

const series_endpoint = "https://agent.datadoghq.com/api/v1/series";
const sketches_endpoint = "https://agent.datadoghq.com/api/beta/sketches";
//const series_endpoint = "http://localhost:8080";
//const sketches_endpoint = "http://localhost:8080";

const headerContentTypeJson: [*:0]const u8 = "Content-Type: application/json";
const headerContentTypeProto: [*:0]const u8 = "Content-Type: application/x-protobuf";

const compressionType = enum {
    Gzip,
    Zlib,
};

const max_retry_per_transaction = 5;
// TODO(remy): instead of limiting on the amount of transactions, we should limit
// on the RAM usage of these stored transactions, i.e. max_stored_transactions_ram_usage
// or something like this.
const max_stored_transactions = 10;

pub const ForwarderError = error{RequestFailed};

/// Transaction is the content of the HTTP request sent to Datadog.
/// If sending a transaction to the intake fails, we'll keep it in RAM for some
/// time to do some retries.
pub const Transaction = struct {
    allocator: std.mem.Allocator,
    compression_type: compressionType = .Zlib,
    // TODO(remy): when not using curl anymore, these ones won't have to be finished by a \0
    content_type: [*:0]const u8,
    // TODO(remy): when not using curl anymore, these ones won't have to be finished by a \0
    url: [*:0]const u8,
    data: std.ArrayListUnmanaged(u8),
    bucket: u64,
    tries: u8,

    // TODO(remy): this shouldn't return a pointer
    pub fn init(allocator: std.mem.Allocator, url: [*:0]const u8, content_type: [*:0]const u8, compression_type: compressionType, bucket: u64) !*Transaction {
        const rv = try allocator.create(Transaction);
        rv.* = Transaction{
            .allocator = allocator,
            .content_type = content_type,
            .compression_type = compression_type,
            .data = .empty,
            .bucket = bucket,
            .tries = 0,
            .url = url,
        };
        return rv;
    }

    pub fn deinit(self: *Transaction) void {
        self.data.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn compress(self: *Transaction) !void {
        var compressed = std.ArrayListUnmanaged(u8).empty;
        var reader = std.io.fixedBufferStream(self.data.items);
        switch (self.compression_type) {
            .Gzip => try std.compress.gzip.compress(reader.reader(), compressed.writer(self.allocator), .{}),
            .Zlib => try std.compress.zlib.compress(reader.reader(), compressed.writer(self.allocator), .{}),
        }
        self.data.deinit(self.allocator);
        self.data = compressed;
    }
};

pub const Forwarder = struct {
    gpa: std.mem.Allocator,
    config: Config,
    consts: struct {
        apikey_header: [:0]const u8,
        series_url: [:0]const u8,
        sketches_url: [:0]const u8,
    },
    // TODO(remy): this could be a specific type owning all the transactions memory
    //             and having all of them stored in an arena managed by the type instead.
    transactions: std.ArrayListUnmanaged(*Transaction),

    pub fn init(gpa: std.mem.Allocator, config: Config) !Forwarder {
        return .{
            .gpa = gpa,
            .config = config,
            .consts = .{
                .apikey_header = try std.fmt.allocPrintZ(gpa, "Dd-Api-Key: {s}", .{config.apikey}),
                .series_url = try std.fmt.allocPrintZ(gpa, "{s}?api_key={s}", .{ series_endpoint, config.apikey }),
                .sketches_url = try std.fmt.allocPrintZ(gpa, "{s}?api_key={s}", .{ sketches_endpoint, config.apikey }),
            },
            // TODO(remy): maybe an arena dedicated to transactions?
            .transactions = .empty,
        };
    }

    pub fn deinit(self: *Forwarder) void {
        self.gpa.free(self.consts.apikey_header);
        self.gpa.free(self.consts.series_url);
        self.gpa.free(self.consts.sketches_url);
        for (self.transactions.items) |tx| {
            tx.deinit();
        }
        self.transactions.deinit(self.gpa);
    }

    /// flush is responsible for sending all the given metrics to some HTTP route.
    pub fn flush(self: *Forwarder, current_bucket: u64, series: std.AutoArrayHashMapUnmanaged(u64, Serie), dists: std.AutoArrayHashMapUnmanaged(u64, Distribution)) !void {
        defer std.log.debug("transactions stored in RAM: {}", .{self.transactions.items.len});

        // try to send a new transaction only if there is metrics to send
        // ---

        if (series.count() > 0) {
            const tx = try self.create_series_transaction(series.values(), current_bucket);
            if (self.send_transaction(tx)) {
                tx.deinit();
            }
        }

        if (dists.count() > 0) {
            const tx = try self.create_sketches_transaction(dists.values(), current_bucket);
            if (self.send_transaction(tx)) {
                tx.deinit();
            }
        }

        // try to replay some older transactions.
        // ---

        if (self.transactions.items.len > 0) {
            // don't replay more than 3 transactions
            self.replay_old_transactions(3);
        }
    }

    // TODO(remy): comment me
    fn send_transaction(self: *Forwarder, tx: *Transaction) bool {
        // try to send the transaction
        self.send_http_request(tx) catch |err| {
            std.log.warn("can't send a transaction: {s}\nstoring the transaction of size {d} bytes [{s}]", .{ @errorName(err), tx.data.items.len, tx.data.items });

            // limit the amount of transactions stored
            if (self.transactions.items.len > max_stored_transactions) {
                std.log.warn("too many transactions stored, removing a random old one.", .{});
                var droppedtx = self.transactions.orderedRemove(0);
                droppedtx.deinit();
            }

            self.transactions.append(self.gpa, tx) catch |err2| {
                std.log.warn("can't store the failing transaction {s}", .{@errorName(err2)});
                tx.deinit();
            };
            return false;
        };

        return true;
    }

    /// creates a transaction with the given distribution series.
    /// This endpoint does not seem to work with Gzip compression.
    fn create_sketches_transaction(self: *Forwarder, dists: []Distribution, bucket: u64) !*Transaction {
        var tx = try Transaction.init(
            self.gpa,
            self.consts.sketches_url,
            headerContentTypeProto,
            .Zlib,
            bucket,
        );

        // create the protobuf payload
        var pb = try protobuf.SketchesFromDistributions(self.gpa, self.config, dists, bucket);
        defer pb.deinit();
        const data = try pb.encode(self.gpa);
        try tx.data.appendSlice(self.gpa, data); // FIXME(remy): avoid this copy
        self.gpa.free(data);

        // compress the transaction
        try tx.compress();

        return tx;
    }

    /// creates a transaction with the given metric series.
    /// This endpoint works with both Gzip and Zlib compression.
    fn create_series_transaction(self: *Forwarder, series: []Serie, bucket: u64) !*Transaction {
        var tx = try Transaction.init(
            self.gpa,
            self.consts.series_url,
            headerContentTypeJson,
            .Gzip,
            bucket,
        );

        try tx.data.appendSlice(self.gpa, "{\"series\":[");

        // append every serie

        var first: bool = true;
        for (series) |serie| {
            if (!first) {
                try tx.data.append(self.gpa, ',');
            }
            first = false;
            try self.write_serie(tx, serie);
        }

        try tx.data.appendSlice(self.gpa, "]}");

        // compress the transaction
        try tx.compress();

        return tx;
    }

    fn replay_old_transactions(self: *Forwarder, maxReplayed: usize) void {
        var i: usize = 0;
        while (i < maxReplayed) : (i += 1) {
            if (self.transactions.items.len == 0) {
                break;
            }
            var tx = self.transactions.orderedRemove(0);
            self.send_http_request(tx) catch |err| {
                std.log.warn("error while retrying a transaction: {s}", .{@errorName(err)});
                if (tx.tries < max_retry_per_transaction) {
                    tx.tries += 1;
                    self.transactions.append(self.gpa, tx) catch |err2| {
                        tx.deinit();
                        std.log.err("can't store the failing transaction {s}", .{@errorName(err2)});
                    };
                } else {
                    tx.deinit();
                    std.log.err("this transaction of {d} bytes dating from {d} has been dropped.", .{ tx.data.items.len, tx.bucket });
                }
                break; // useless to try more transaction right now
            };
        }
    }

    // TODO(remy): implement a separate serializer
    // TODO(remy): comment
    // TODO(remy): unit test
    fn write_serie(self: *Forwarder, tx: *Transaction, serie: Serie) !void {
        // {
        //   "metric": "system.mem.used",
        //   "points": [
        //     [
        //       1589122593,
        //       1811.29296875
        //     ]
        //   ],
        //   "tags": ["dev:remeh","env:test"],
        //   "host": "hostname",
        //   "type": "gauge",
        //   "interval": 0,
        //   "source_type_name": "System"
        // }

        // metric type string
        const t: []const u8 = switch (serie.metric_type) {
            .Gauge => "gauge",
            .Counter => "count",
            else => unreachable,
        };

        const value: f32 = switch (serie.metric_type) {
            .Gauge => serie.value,
            .Counter => serie.value/@as(f32, @floatFromInt(serie.samples)),
            else => unreachable,
        };

        // tags
        var tags = std.ArrayListUnmanaged(u8).empty;
        var i: usize = 0;
        for (serie.tags.tags.items) |tag| {
            try tags.append(self.gpa, '"');
            try tags.appendSlice(self.gpa, tag);
            try tags.append(self.gpa, '"');
            if (i < serie.tags.tags.items.len - 1) {
                try tags.append(self.gpa, ',');
            }
            i += 1;
        }
        defer tags.deinit(self.gpa);

        // build the json
        const json = try std.fmt.allocPrint(
            self.gpa,
            "{{\"metric\":\"{s}\",\"host\":\"{s}\",\"tags\":[{s}],\"type\":\"{s}\",\"points\":[[{d},{d}]],\"interval\":10,\"source_type_name\":\"System\"}}",
            .{
                serie.metric_name,
                self.config.hostname,
                tags.items,
                t,
                tx.bucket,
                value,
            },
        );
        defer self.gpa.free(json);

        // append it to the main buffer
        try tx.*.data.appendSlice(self.gpa, json);
    }

    fn send_http_request(self: *Forwarder, tx: *Transaction) !void {
        if (self.config.force_curl) {
            return self.send_http_request_curl(tx);
        }

        switch (builtin.os.tag) {
            .linux => return self.send_http_request_native(tx),
            else => return self.send_http_request_curl(tx),
        }
    }

    fn send_http_request_curl(self: *Forwarder, tx: *Transaction) !void {
        var failed: bool = false;
        var curl: ?*c.CURL = null;
        var res: c.CURLcode = undefined;
        var headers: [*c]c.curl_slist = null;

        curl = c.curl_easy_init();
        if (curl != null) {
            // url
            _ = c.curl_easy_setopt(curl, c.CURLOPT_URL, @as([*:0]const u8, @ptrCast(tx.url)));

            // body
            _ = c.curl_easy_setopt(curl, c.CURLOPT_POSTFIELDSIZE, tx.data.items.len);
            _ = c.curl_easy_setopt(curl, c.CURLOPT_POST, @as(c_int, 1));
            _ = c.curl_easy_setopt(curl, c.CURLOPT_POSTFIELDS, @as([*:0]const u8, @ptrCast(tx.data.items)));

            // http headers
            headers = c.curl_slist_append(headers, tx.content_type);
            headers = c.curl_slist_append(headers, "DD-Agent-Payload: 4.87.0"); // TODO(remy): document me
            headers = c.curl_slist_append(headers, "DD-Agent-Version: 7.40.0"); // TODO(remy): document me
            switch (tx.compression_type) {
                .Gzip => headers = c.curl_slist_append(headers, "Content-Encoding: gzip"),
                // When using zlib compression, the Content-Encoding header should
                // have the `deflate`  value...
                // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
                // Don't ask my why it's not the deflate compression who uses the deflate value,
                // I don't make the rules.
                .Zlib => headers = c.curl_slist_append(headers, "Content-Encoding: deflate"),
            }
            headers = c.curl_slist_append(headers, "User-Agent: datadog-agent/7.40.0");
            headers = c.curl_slist_append(headers, self.consts.apikey_header);
            _ = c.curl_easy_setopt(curl, c.CURLOPT_HTTPHEADER, headers);

            // perform the call
            res = c.curl_easy_perform(curl);
            if (res != @as(c_uint, @bitCast(c.CURLE_OK))) {
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
            std.log.debug("http flush done, request payload size: {}", .{tx.data.items.len});
        }
    }

    fn send_http_request_native(self: *Forwarder, tx: *Transaction) !void {
        // http client
        var http_client = std.http.Client{ .allocator = self.gpa };
        defer http_client.deinit();

        // response
        var resp = std.ArrayList(u8).init(self.gpa);
        defer resp.deinit();

        // TODO(remy): remove me when not supporting curl anymore
        const url = tx.url[0..std.mem.len(tx.url)];
        const content_type = tx.content_type[0..std.mem.len(tx.content_type)];

        const req_opts = std.http.Client.FetchOptions{
            .location = .{ .uri = try std.Uri.parse(url) },
            .method = .POST,
            // .redirect_behavior = .unhandled,
            .payload = tx.data.items,
            .response_storage = .{ .dynamic = &resp },
            // FIXME(remy): compression header
            .extra_headers = &.{
                std.http.Header{ .name = "Dd-Api-Key", .value = self.config.apikey },
                std.http.Header{ .name = "Content-Type", .value = content_type },
                std.http.Header{ .name = "DD-Agent-Payload", .value = "4.87.0" }, // TODO(remy): document me
                std.http.Header{ .name = "DD-Agent-Version", .value = "7.40.0" }, // TODO(remy): document me
                std.http.Header{
                    .name = "Content-Encoding",
                    .value = switch (tx.compression_type) {
                        .Gzip => "gzip",
                        // When using zlib compression, the HTTP Content-Encoding header should
                        // have the value `deflate`...
                        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
                        // Don't ask my why it's not the deflate compression who uses the deflate value,
                        // I don't make the rules.
                        .Zlib => "deflate",
                    },
                },
                std.http.Header{ .name = "User-Agent", .value = "datadog-agent/7.40.0" },
            },
        };

        const r = http_client.fetch(req_opts) catch |err| {
            std.log.err("send_http_request_native: on fetch: {}", .{err});
            return;
        };

        switch (r.status) {
            .ok, .accepted => {},
            else => {
                std.log.err("send_http_request_native: http response error: {any}", .{r.status});
            },
        }
    }
};

test "transaction_mem_usage" {
    const allocator = std.testing.allocator;
    const name = try allocator.alloc(u8, "my.metric".len);
    std.mem.copyForwards(u8, name, "my.metric");

    const serie = Serie{
        .metric_name = name,
        .metric_type = .Gauge,
        .samples = 1,
        .value = 1,
        .tags = .empty,
    };

    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };

    var series = std.AutoArrayHashMapUnmanaged(u64, Serie).empty;
    try series.put(allocator, 123456789, serie);

    var forwarder = try Forwarder.init(allocator, config);
    defer forwarder.deinit();

    var tx = try forwarder.create_series_transaction(series.values(), 0);
    tx.deinit();

    series.deinit(allocator);
    allocator.free(name);
}

// TODO(remy): add a test for replay_old_transactions

// TODO(remy): add a test for some json complete serialization

test "write_serie_test" {
    const allocator = std.testing.allocator;
    var tx = try Transaction.init(allocator, series_endpoint, headerContentTypeJson, .Zlib, 0);

    const name = try allocator.alloc(u8, "my.metric".len);
    std.mem.copyForwards(u8, name, "my.metric");

    const serie = Serie{
        .metric_name = name,
        .metric_type = .Gauge,
        .samples = 1,
        .value = 1,
        .tags = .empty,
    };

    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };

    var forwarder = try Forwarder.init(allocator, config);
    defer forwarder.deinit();

    try forwarder.write_serie(tx, serie);
    tx.deinit();

    allocator.free(name);
}
