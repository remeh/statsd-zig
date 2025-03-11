const std = @import("std");
const mem = std.mem;
const warn = std.log.warn;
const fnv1a = std.hash.Fnv1a_64;
const assert = std.debug.assert;

const metric = @import("metric.zig");
const Config = @import("config.zig").Config;
const DDSketch = @import("ddsketch.zig").DDSketch;
const Parser = @import("parser.zig").Parser; // used in tests
const Forwarder = @import("forwarder.zig").Forwarder;
const TagsSetUnmanaged = @import("metric.zig").TagsSetUnmanaged;
const Transaction = @import("forwarder.zig").Transaction;

// TODO(remy): comment me
const sampling_interval: u64 = 10;

// TODO(remy): comment me
pub const Serie = struct {
    metric_name: []const u8,
    metric_type: metric.MetricType,
    tags: TagsSetUnmanaged,
    samples: u64,
    value: f32,

    pub fn deinit(self: *Serie, allocator: std.mem.Allocator) void {
        self.tags.deinit(allocator);
    }
};

// TODO(remy): comment me
pub const Distribution = struct {
    metric_name: []const u8,
    tags: TagsSetUnmanaged,
    sketch: DDSketch,

    pub fn deinit(self: *Distribution, allocator: std.mem.Allocator) void {
        self.tags.deinit(allocator);
        self.sketch.deinit();
    }
};

/// A bucket is a set of sampled series and distributions for a given 10s interval.
/// A sampler will use several buckets in order to be able to do a double-buffering
/// mechanism, but also to keep track of events received with a timestamp in the past.
/// All the memory of the objects owned by this bucket is part of the arena.
const Bucket = struct {
    arena: std.heap.ArenaAllocator,
    interval_start: u64 = 0, // start timestamp of the interval, aligned to 10s
    interval: u64 = 10,
    // TODO(remy): benchmark against std.AutoHashMapUnmanaged
    distributions: std.AutoArrayHashMapUnmanaged(u64, Distribution),
    series: std.AutoArrayHashMapUnmanaged(u64, Serie),
    mutex: std.Thread.Mutex,

    pub fn init(gpa: std.mem.Allocator, interval_start: u64, interval: u64) !*Bucket {
        const bucket = try gpa.create(Bucket);
        bucket.* = .{
            .arena = std.heap.ArenaAllocator.init(gpa),
            .interval_start = interval_start,
            .interval = interval,
            .distributions = .empty,
            .series = .empty,
            .mutex = std.Thread.Mutex{},
        };
        return bucket;
    }

    pub fn deinit(self: *Bucket, gpa: std.mem.Allocator) void {
        self.distributions = .empty;
        self.series = .empty;
        _ = self.arena.reset(.free_all);
        gpa.destroy(self);
    }

    // TODO(remy): comment me
    pub fn key(timestamp: i64) u64 {
        return @intCast(@divTrunc(timestamp, 10) * 10);
    }

    /// size returns how many different series + distributions are sampled in this bucket.
    pub fn size(self: *Bucket) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.series.count() + self.distributions.count();
    }
};

// TODO(remy): comment me
pub const Sampler = struct {
    /// used to create/destroy the bucket entries
    gpa: std.mem.Allocator,
    config: Config,
    forwarder: Forwarder,

    buckets: std.AutoHashMapUnmanaged(u64, *Bucket),
    mutex: std.Thread.Mutex,

    pub fn init(gpa: std.mem.Allocator, config: Config) !Sampler {
        return Sampler{
            .buckets = .empty,
            .gpa = gpa,
            .mutex = std.Thread.Mutex{},
            .config = config,
            .forwarder = try Forwarder.init(gpa, config),
        };
    }

    /// destroy frees all the memory used by the Sampler and the instance itself.
    /// The Sampler instance should not be used anymore after a call to destroy.
    pub fn deinit(self: *Sampler) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var it = self.buckets.valueIterator();
        while (it.next()) |bucket| {
            bucket.*.deinit(self.gpa);
        }
        self.buckets.deinit(self.gpa);

        self.forwarder.deinit();
    }

    /// Not thread-safe.
    pub fn current_bucket(self: *Sampler) !*Bucket {
        const bucket_key = Bucket.key(std.time.timestamp());
        if (self.buckets.get(bucket_key)) |bucket| {
            return bucket;
        }

        const bucket = try Bucket.init(self.gpa, bucket_key, sampling_interval);
        try self.buckets.put(self.gpa, bucket_key, bucket);
        std.log.debug("creating bucket for {d}", .{bucket_key});
        return bucket;
    }

    pub fn sample(self: *Sampler, m: metric.Metric) !void {
        self.mutex.lock();
        const bucket = try self.current_bucket();
        self.mutex.unlock();

        const h = Sampler.hash(m);

        if (m.type == .Distribution) {
            return try self.sampleDistribution(bucket, m, h);
        }

        return try self.sampleSerie(bucket, m, h);
    }

    /// sample internal telemetry about the server itself.
    /// Won't return an error but throw debug logs instead.
    pub fn sampleTelemetry(self: *Sampler, metric_type: metric.MetricType, name: []const u8, value: f32, tags: TagsSetUnmanaged) void {
        self.sample(metric.Metric{
            .allocator = undefined,
            .name = name,
            .value = value,
            .type = metric_type,
            .tags = tags,
        }) catch |err| {
            std.log.err("can't report server telemetry '{s}': {}", .{ name, err });
        };
    }

    pub fn sampleDistribution(_: *Sampler, bucket: *Bucket, m: metric.Metric, h: u64) !void {
        bucket.mutex.lock();
        defer bucket.mutex.unlock();
        const k = bucket.distributions.get(h);

        if (k) |d| {
            // existing
            var newDistribution = d;
            try newDistribution.sketch.insert(@floatCast(m.value));
            try bucket.distributions.put(bucket.arena.allocator(), h, newDistribution);
            return;
        }

        // not existing

        const name = try bucket.arena.allocator().alloc(u8, m.name.len);
        std.mem.copyForwards(u8, name, m.name);

        // TODO(remy): could we steal these tags from the metric instead?
        var tags = TagsSetUnmanaged.empty;
        for (m.tags.tags.items) |tag| {
            try tags.append(bucket.arena.allocator(), tag);
        }

        const sketch = DDSketch.initDefault(bucket.arena.allocator());

        try bucket.distributions.put(bucket.arena.allocator(), h, Distribution{
            .metric_name = name,
            .tags = tags,
            .sketch = sketch,
        });
        return;
    }

    pub fn sampleSerie(_: *Sampler, bucket: *Bucket, m: metric.Metric, h: u64) !void {
        bucket.mutex.lock();
        defer bucket.mutex.unlock();
        const k = bucket.series.get(h);

        if (k) |s| {
            var newSerie = s;
            newSerie.samples += 1;

            switch (s.metric_type) {
                .Gauge => newSerie.value = m.value,
                else => newSerie.value += m.value, // Counter
            }

            try bucket.series.put(bucket.arena.allocator(), h, newSerie);
            return;
        }

        // not existing

        const name = try bucket.arena.allocator().alloc(u8, m.name.len);
        std.mem.copyForwards(u8, name, m.name);

        // TODO(remy): instead of copying, could we steal these tags from the metric?
        var tags = TagsSetUnmanaged.empty;
        for (m.tags.tags.items) |tag| {
            try tags.append(bucket.arena.allocator(), tag);
        }

        try bucket.series.put(bucket.arena.allocator(), h, Serie{
            .metric_name = name,
            .metric_type = m.type,
            .samples = 1,
            .tags = tags,
            .value = m.value,
        });
    }

    pub fn flush(self: *Sampler) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var buckets_to_flush: std.ArrayListUnmanaged(u64) = .empty;
        defer buckets_to_flush.deinit(self.gpa);

        var it = self.buckets.keyIterator();
        while (it.next()) |key| {
            if (key.* + sampling_interval <= Bucket.key(std.time.timestamp())) {
                try buckets_to_flush.append(self.gpa, key.*);
            }
        }

        for (buckets_to_flush.items) |key| {
            if (self.buckets.fetchRemove(key)) |kv| {
                const bucket = kv.value;
                std.log.debug("flushing bucket {d}", .{bucket.interval_start});
                try self.forwarder.new_transaction(bucket.interval_start, bucket.series, bucket.distributions);
                bucket.deinit(self.gpa);
            }
        }
    }

    fn hash(m: metric.Metric) u64 {
        var h = fnv1a.init();
        h.update(m.name);
        for (m.tags.tags.items) |tag| {
            h.update(tag);
        }
        return h.final();
    }
};

// TODO(remy): test buckets implementation

test "sampling hashing" {
    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };

    const allocator = std.testing.allocator;

    var sampler = try Sampler.init(allocator, config);
    const tags = try Parser.parse_tags(allocator, "#my:tag,second:tag");
    var m = try metric.Metric.init(allocator, "this.is.my.metric");
    defer m.deinit();
    m.value = 50.0;
    m.type = .Counter;
    m.tags = tags;

    try sampler.sample(m);
    assert((try sampler.current_bucket()).size() == 1);

    try sampler.sample(m);
    assert((try sampler.current_bucket()).size() == 1);

    var m2 = try metric.Metric.init(allocator, "this.is.my.metric");
    const tags2 = try Parser.parse_tags(allocator, "#my:tag,second:tag");
    defer m2.deinit();
    m2.value = 25.0;
    m2.type = .Counter;
    m2.tags = tags2;

    try sampler.sample(m2);
    assert((try sampler.current_bucket()).size() == 1);

    var m3 = try metric.Metric.init(allocator, "this.is.my.other.metric");
    const tags3 = try Parser.parse_tags(allocator, "#my:tag,second:tag");
    defer m3.deinit();
    m3.value = 25.0;
    m3.type = .Counter;
    m3.tags = tags3;

    try sampler.sample(m3);
    assert((try sampler.current_bucket()).size() == 2);

    var other_tags = try Parser.parse_tags(allocator, "#my:tag,second:tag,and:other");
    other_tags.deinit(allocator);

    sampler.deinit();
}

test "sampling gauge" {
    const allocator = std.testing.allocator;

    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };

    var sampler = try Sampler.init(allocator, config);

    var m = try metric.Metric.init(allocator, "this.is.my.gauge");
    defer m.deinit();
    m.value = 50.0;
    m.type = .Gauge;
    m.tags = .empty;

    try sampler.sample(m);
    try std.testing.expectEqual(1, (try sampler.current_bucket()).size());

    var current_bucket = try sampler.current_bucket();
    for (current_bucket.series.values()) |serie| {
        try std.testing.expectEqual(50.0, serie.value);
    }

    m.value = 20;
    try sampler.sample(m);
    try std.testing.expectEqual(1, (try sampler.current_bucket()).size());

    current_bucket = try sampler.current_bucket();
    for (current_bucket.series.values()) |serie| {
        try std.testing.expectEqual(20.0, serie.value);
    }

    sampler.deinit();
}

test "sampling counter" {
    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };
    const allocator = std.testing.allocator;

    var sampler = try Sampler.init(allocator, config);
    var m = try metric.Metric.init(allocator, "this.is.my.counter");
    defer m.deinit();
    m.value = 50.0;
    m.type = .Counter;
    m.tags = .empty;

    try sampler.sample(m);
    try std.testing.expectEqual(1, (try sampler.current_bucket()).size());

    var current_bucket = try sampler.current_bucket();
    var iterator = current_bucket.series.iterator();
    if (iterator.next()) |kv| {
        const serie = kv.value_ptr.*;
        assert(serie.value == 50.0);
        assert(serie.samples == 1);
    }

    m.value = 20;

    try sampler.sample(m);
    current_bucket = try sampler.current_bucket();
    for (current_bucket.series.values()) |serie| {
        assert(serie.value == 70.0);
        assert(serie.samples == 2);
    }

    sampler.deinit();
}
