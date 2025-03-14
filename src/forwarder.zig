const std = @import("std");
const builtin = @import("builtin");

const AtomicQueue = @import("atomic_queue.zig").AtomicQueue;
const Distribution = @import("sampler.zig").Distribution;
const Config = @import("config.zig").Config;
const Serie = @import("sampler.zig").Serie;
const Signal = @import("signal.zig").Signal;

const metric = @import("metric.zig");
const protobuf = @import("protobuf.zig");

const series_endpoint = "https://agent.datadoghq.com/api/v1/series";
const sketches_endpoint = "https://agent.datadoghq.com/api/beta/sketches";
//const series_endpoint = "http://localhost:8080";
//const sketches_endpoint = "http://localhost:8080";

const headerContentTypeJson: []const u8 = "Content-Type: application/json";
const headerContentTypeProto: []const u8 = "Content-Type: application/x-protobuf";

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

/// The Forwarder is the entrypoint to send data to a remote intake.
///
/// Its job is to receive Buckets to process, to serialize their content into
/// a transaction, and it is pushing this transaction to a separate thread responsible
/// of sending these transactions to the remote intake.
/// It owns and runs a separate thread, which functions are part of ForwarderThread.
pub const Forwarder = struct {
    gpa: std.mem.Allocator,
    config: Config,
    consts: struct {
        apikey_header: []const u8,
        series_url: []const u8,
        sketches_url: []const u8,
    },
    pthread: std.Thread,
    thread: *ForwarderThread,

    pub fn init(gpa: std.mem.Allocator, config: Config) !Forwarder {
        var forwarder = Forwarder{
            .gpa = gpa,
            .config = config,
            .consts = .{
                .apikey_header = try std.fmt.allocPrint(gpa, "Dd-Api-Key: {s}", .{config.apikey}),
                .series_url = try std.fmt.allocPrint(gpa, "{s}?api_key={s}", .{ series_endpoint, config.apikey }),
                .sketches_url = try std.fmt.allocPrint(gpa, "{s}?api_key={s}", .{ sketches_endpoint, config.apikey }),
            },
            .pthread = undefined,
            .thread = undefined,
        };

        // configure and spawn the sending thread
        const thread_context = try gpa.create(ForwarderThread);
        thread_context.* = .{
            .backlog = .empty,
            .q = AtomicQueue(*Transaction).init(),
            .config = config,
            .signal = try Signal.init(),
            .gpa = gpa,
            .running = std.atomic.Value(bool).init(true),
            .consts = .{
                .apikey_header = forwarder.consts.apikey_header,
            },
        };
        forwarder.pthread = try std.Thread.spawn(std.Thread.SpawnConfig{}, ForwarderThread.run, .{thread_context});
        forwarder.thread = thread_context;

        return forwarder;
    }

    pub fn deinit(self: *Forwarder) void {
        self.gpa.free(self.consts.apikey_header);
        self.gpa.free(self.consts.series_url);
        self.gpa.free(self.consts.sketches_url);

        while (self.thread.q.get()) |node| {
            const tx = node.data;
            tx.deinit();
            self.gpa.destroy(node);
        }

        self.thread.running.store(false, .release);
        self.pthread.join();
        self.gpa.destroy(self.thread);
    }

    /// new_transaction creates a transaction for the given metrics
    /// and pushes it to the sending thread.
    pub fn new_transaction(self: *Forwarder, current_bucket: u64, series: std.AutoArrayHashMapUnmanaged(u64, Serie), dists: std.AutoArrayHashMapUnmanaged(u64, Distribution)) !void {
        var something_to_send = false;

        if (series.count() > 0) {
            const tx = try self.create_series_transaction(series.values(), current_bucket);
            var node = try self.gpa.create(AtomicQueue(*Transaction).Node);
            node.data = tx;
            self.thread.q.put(node);
            something_to_send = true;
        }

        if (dists.count() > 0) {
            const tx = try self.create_sketches_transaction(dists.values(), current_bucket);
            var node = try self.gpa.create(AtomicQueue(*Transaction).Node);
            node.data = tx;
            self.thread.q.put(node);
            something_to_send = true;
        }

        if (something_to_send) {
            try self.thread.signal.emit();
        }
    }

    /// creates a transaction with the given distribution series.
    /// This endpoint does not seem to work with Gzip compression.
    fn create_sketches_transaction(self: *Forwarder, dists: []Distribution, bucket: u64) !*Transaction {
        const now = std.time.milliTimestamp();
        defer {
            const elapsed = std.time.milliTimestamp() - now;
            std.log.debug("time to serialze sketches transaction: {d}ms", .{elapsed});
            // TODO(remy): how to send any telemetry here?
        }

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

        // to avoid a copy, directly use the data from the encoding
        // it is then owned by the Transaction which will clean it up
        // once not necessary anymore.
        const encoded = try pb.encode(self.gpa);
        tx.data.items = encoded;
        tx.data.capacity = encoded.len;

        // compress the transaction
        try tx.compress();

        return tx;
    }

    /// creates a transaction with the given metric series.
    /// This endpoint works with both Gzip and Zlib compression.
    fn create_series_transaction(self: *Forwarder, series: []Serie, bucket: u64) !*Transaction {
        const now = std.time.milliTimestamp();
        defer {
            const elapsed = std.time.milliTimestamp() - now;
            std.log.debug("time to serialze series transaction: {d}ms", .{elapsed});
            // TODO(remy): how to send any telemetry here?
        }

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
            // FIXME(remy): this seems to be strangely inaccurate when we have
            // a VERY LARGE amount of samples (millions of samples for the same
            // metric, e.g. a same metric sent 200k times per second).
            // Could it be because of the f32 precision?
            .Counter => serie.value / @as(f32, @floatFromInt(serie.samples)),
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
};

// TODO(remy): comment me
const ForwarderThread = struct {
    q: AtomicQueue(*Transaction),
    signal: Signal,
    config: Config,
    gpa: std.mem.Allocator,
    backlog: std.ArrayListUnmanaged(*Transaction),
    running: std.atomic.Value(bool),
    consts: struct {
        // data owned by the main Forwarder instance.
        apikey_header: []const u8,
    },

    fn run(self: *ForwarderThread) void {
        std.log.debug("starting forwarder thread", .{});
        var running = true;
        while (running) {
            self.signal.wait(1000) catch |err| {
                std.log.debug("can't wait for a signal, will sleep instead: {}", .{err});
                std.time.sleep(1*std.time.ns_per_s);
            };

            if (!self.running.load(.acquire)) {
                running = false;
            }

            // is there something to process
            while (!self.q.isEmpty()) {
                if (self.q.get()) |node| {
                    const tx = node.data;

                    self.send_http_request(tx) catch |err| {
                        std.log.warn("can't send a transaction: {s}\nstoring the transaction of size {d} bytes [{s}]", .{ @errorName(err), tx.data.items.len, tx.data.items });

                        // limit the amount of transactions stored
                        if (self.backlog.items.len > max_stored_transactions) {
                            std.log.warn("too many transactions stored, removing a random old one.", .{});
                            var droppedtx = self.backlog.orderedRemove(0);
                            droppedtx.deinit();
                        }

                        self.backlog.append(self.gpa, tx) catch |err2| {
                            std.log.warn("can't store the failing transaction {s}", .{@errorName(err2)});
                            tx.deinit();
                        };

                        continue; // FIXME(remy): is this applying the continue to the while as intended?
                    };

                    std.log.debug("sent transaction size {d} bytes", .{tx.data.items.len});
                    tx.deinit();
                    self.gpa.destroy(node);
                }
            }

            self.replay_backlog_transactions(3);
        }
    }

    fn send_http_request(self: *ForwarderThread, tx: *Transaction) !void {
        // http client
        var http_client = std.http.Client{ .allocator = self.gpa };
        defer http_client.deinit();

        // response
        var resp = std.ArrayList(u8).init(self.gpa);
        defer resp.deinit();

        const req_opts = std.http.Client.FetchOptions{
            .location = .{ .uri = try std.Uri.parse(tx.url) },
            .method = .POST,
            // .redirect_behavior = .unhandled,
            .payload = tx.data.items,
            .response_storage = .{ .dynamic = &resp },
            .extra_headers = &.{
                std.http.Header{ .name = "Dd-Api-Key", .value = self.config.apikey },
                std.http.Header{ .name = "Content-Type", .value = tx.content_type },
                // the protobuf definition used has been shipped with the
                // agent 7.40.0 (and is version 4.87.0), we let the backend
                // know about this with these two following headers.
                std.http.Header{ .name = "DD-Agent-Payload", .value = "4.87.0" },
                std.http.Header{ .name = "DD-Agent-Version", .value = "7.40.0" },

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

    fn replay_backlog_transactions(self: *ForwarderThread, maxReplayed: usize) void {
        var i: usize = 0;
        while (i < maxReplayed) : (i += 1) {
            if (self.backlog.items.len == 0) {
                break;
            }
            var tx = self.backlog.orderedRemove(0);
            self.send_http_request(tx) catch |err| {
                std.log.warn("error while retrying a transaction: {s}", .{@errorName(err)});
                if (tx.tries < max_retry_per_transaction) {
                    tx.tries += 1;
                    self.backlog.append(self.gpa, tx) catch |err2| {
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
};

/// Transaction is the content of the HTTP request sent to the intake.
/// If sending a transaction to the intake fails, we'll keep it in RAM for some
/// time to do some retries.
pub const Transaction = struct {
    allocator: std.mem.Allocator,
    compression_type: compressionType = .Zlib,
    content_type: []const u8,
    url: []const u8,
    data: std.ArrayListUnmanaged(u8),
    bucket: u64,
    tries: u8,

    pub fn init(allocator: std.mem.Allocator, url: []const u8, content_type: []const u8, compression_type: compressionType, bucket: u64) !*Transaction {
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
