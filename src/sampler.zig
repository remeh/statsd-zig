const std = @import("std");
const mem = std.mem;
const warn = std.log.warn;
const fnv1a = std.hash.Fnv1a_64;
const assert = std.debug.assert;

const Event = @import("event.zig").Event;
const EventType = @import("event.zig").EventType;
const Config = @import("config.zig").Config;
const DDSketch = @import("ddsketch.zig").DDSketch;
const EventErrors = @import("event.zig").EventErrors;
const Parser = @import("parser.zig").Parser; // used in tests
const Forwarder = @import("forwarder.zig").Forwarder;
const TagsSetUnmanaged = @import("event.zig").TagsSetUnmanaged;
const Transaction = @import("forwarder.zig").Transaction;

/// default sampling interval is 10s.
pub const sampling_interval: u64 = 10;

/// Serie is a metric of type gauge or counter.
pub const Serie = struct {
    metric_name: []const u8,
    metric_type: EventType,
    tags: TagsSetUnmanaged,
    value: f64,

    pub fn deinit(self: *Serie, allocator: std.mem.Allocator) void {
        allocator.free(self.metric_name);
        self.tags.deinit(allocator);
    }
};

/// Distribution is a metric of type distribution, stored in a DDSketch.
pub const Distribution = struct {
    metric_name: []const u8,
    tags: TagsSetUnmanaged,
    sketch: DDSketch,

    pub fn deinit(self: *Distribution, allocator: std.mem.Allocator) void {
        allocator.free(self.metric_name);
        self.tags.deinit(allocator);
        self.sketch.deinit();
    }
};

/// A bucket is a set of sampled series and distributions for a given 10s interval.
/// A sampler will use several buckets in order to be able to do a double-buffering
/// mechanism, but also to keep track of events received with a timestamp in the past.
/// All the memory of the objects owned by this bucket is part of the arena.
const Bucket = struct {
    gpa: std.mem.Allocator,
    interval_start: u64 = 0, // start timestamp of the interval, aligned to 10s
    interval: u64 = 10,
    // TODO(remy): benchmark against std.AutoHashMapUnmanaged
    distributions: std.AutoArrayHashMapUnmanaged(u64, Distribution),
    series: std.AutoArrayHashMapUnmanaged(u64, Serie),
    mutex: std.Thread.Mutex,

    pub fn init(gpa: std.mem.Allocator, interval_start: u64, interval: u64) !*Bucket {
        const bucket = try gpa.create(Bucket);
        bucket.* = .{
            .gpa = gpa,
            .interval_start = interval_start,
            .interval = interval,
            .distributions = .empty,
            .series = .empty,
            .mutex = std.Thread.Mutex{},
        };
        return bucket;
    }

    pub fn deinit(self: *Bucket) void {
        for (self.distributions.values()) |*dist| {
            dist.deinit(self.gpa);
        }
        self.distributions.deinit(self.gpa);
        for (self.series.values()) |*serie| {
            serie.deinit(self.gpa);
        }
        self.series.deinit(self.gpa);
        self.distributions = .empty;
        self.series = .empty;
        self.gpa.destroy(self);
    }

    /// key returns a key aligning the timestamp on the sampling interval.
    pub fn key(timestamp: i64) u64 {
        return @intCast(@divTrunc(timestamp, sampling_interval) * sampling_interval);
    }

    /// size returns how many different series + distributions are sampled in this bucket.
    pub fn size(self: *Bucket) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.series.count() + self.distributions.count();
    }
};

/// Sampler receives gauges, counters and distributions, and aggregates them
/// into a given window interval (10s by default).
/// The sampler also owns a forwarder, into which are flushed the aggregated
/// series and sketches to send them to the intake.
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

        self.forwarder.deinit();

        var it = self.buckets.valueIterator();
        while (it.next()) |bucket| {
            bucket.*.deinit();
        }
        self.buckets.deinit(self.gpa);
    }

    /// currentBuckets returns current bucket (based on the current time) in
    /// which should be aggregated series and distributions.
    /// Not thread-safe.
    pub fn currentBucket(self: *Sampler) !*Bucket {
        const bucket_key = Bucket.key(std.time.timestamp());
        if (self.buckets.get(bucket_key)) |bucket| {
            return bucket;
        }

        const bucket = try Bucket.init(self.gpa, bucket_key, sampling_interval);
        try self.buckets.put(self.gpa, bucket_key, bucket);
        std.log.debug("creating bucket for {d}", .{bucket_key});
        return bucket;
    }

    /// sample an event in this sampler.
    /// If the event isn't of a supported type, this method
    /// returns an EventErrors.UnsupportedType.
    pub fn sample(self: *Sampler, m: Event) !void {
        self.mutex.lock();
        const bucket = try self.currentBucket();
        self.mutex.unlock();

        const h = Sampler.hash(m);

        switch (m.type) {
            .Distribution => try self.sampleDistribution(bucket, m, h),
            .Counter, .Gauge => try self.sampleSerie(bucket, m, h),
            else => return EventErrors.UnsupportedType,
        }
    }

    /// sample internal telemetry about the server itself.
    /// Won't return an error but throw debug logs instead.
    pub fn sampleTelemetry(self: *Sampler, metric_type: EventType, name: []const u8, value: f32, tags: TagsSetUnmanaged) void {
        self.sample(Event{
            .name = name,
            .value = value,
            .type = metric_type,
            .tags = tags,
        }) catch |err| {
            std.log.err("can't report server telemetry '{s}': {}", .{ name, err });
        };
    }

    pub fn sampleDistribution(_: *Sampler, bucket: *Bucket, e: Event, h: u64) !void {
        bucket.mutex.lock();
        defer bucket.mutex.unlock();
        const k = bucket.distributions.getPtr(h);

        // existing
        if (k) |d| {
            // existing
            try d.sketch.insert(@floatCast(e.value));
            // we don't need to put back in the map since we used the pointer
            // to modify the sketch directly.
            return;
        }

        // not existing
        const name = try bucket.gpa.alloc(u8, e.name.len);
        std.mem.copyForwards(u8, name, e.name);
        try bucket.distributions.put(bucket.gpa, h, Distribution{
            .metric_name = name,
            // FIXME(remy): for now, we have to copy the tagset since the metric memory
            // lives in the arena of the main thread.
            // If it were to live in the same gpa as one used by the bucket, we
            // would be able to steal these tags instead and not copy them.
            .tags = try e.tags.copy(bucket.gpa),
            .sketch = DDSketch.initDefault(bucket.gpa),
        });
        return;
    }

    pub fn sampleSerie(_: *Sampler, bucket: *Bucket, e: Event, h: u64) !void {
        bucket.mutex.lock();
        defer bucket.mutex.unlock();
        const k = bucket.series.getPtr(h);

        // existing
        if (k) |s| {
            switch (s.metric_type) {
                .Gauge => s.value = e.value,
                else => s.value += e.value, // Counter
            }
            return;
        }

        // not existing
        const name = try bucket.gpa.alloc(u8, e.name.len);
        std.mem.copyForwards(u8, name, e.name);
        try bucket.series.put(bucket.gpa, h, Serie{
            .metric_name = name,
            .metric_type = e.type,
            // FIXME(remy): for now, we have to copy the tagset since the metric memory
            // lives in the arena of the main thread.
            // If it were to live in the same gpa as one used by the bucket, we
            // would be able to steal these tags instead and not copy them.
            .tags = try e.tags.copy(bucket.gpa),
            .value = e.value,
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
                bucket.deinit();
            }
        }
    }

    fn hash(e: Event) u64 {
        var h = fnv1a.init();
        h.update(e.name);
        for (e.tags.tags.items) |tag| {
            h.update(tag);
        }
        return h.final();
    }
};

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
    var m = try Event.initMetric(allocator, "this.is.my.metric");
    defer m.deinit(allocator);
    m.value = 50.0;
    m.type = .Counter;
    m.tags = tags;

    try sampler.sample(m);
    assert((try sampler.currentBucket()).size() == 1);

    try sampler.sample(m);
    assert((try sampler.currentBucket()).size() == 1);

    var m2 = try Event.initMetric(allocator, "this.is.my.metric");
    const tags2 = try Parser.parse_tags(allocator, "#my:tag,second:tag");
    defer m2.deinit(allocator);
    m2.value = 25.0;
    m2.type = .Counter;
    m2.tags = tags2;

    try sampler.sample(m2);
    assert((try sampler.currentBucket()).size() == 1);

    var m3 = try Event.initMetric(allocator, "this.is.my.other.metric");
    const tags3 = try Parser.parse_tags(allocator, "#my:tag,second:tag");
    defer m3.deinit(allocator);
    m3.value = 25.0;
    m3.type = .Counter;
    m3.tags = tags3;

    try sampler.sample(m3);
    assert((try sampler.currentBucket()).size() == 2);

    var other_tags = try Parser.parse_tags(allocator, "#my:tag,second:tag,and:other");
    other_tags.deinit(allocator);

    sampler.deinit();
}

test "sampling gauge" {
    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };

    const allocator = std.testing.allocator;

    var sampler = try Sampler.init(allocator, config);

    var m = try Event.initMetric(allocator, "this.is.my.gauge");
    defer m.deinit(allocator);
    m.value = 50.0;
    m.type = .Gauge;
    m.tags = .empty;

    try sampler.sample(m);
    try std.testing.expectEqual(1, (try sampler.currentBucket()).size());

    var currentBucket = try sampler.currentBucket();
    for (currentBucket.series.values()) |serie| {
        try std.testing.expectEqual(50.0, serie.value);
    }

    m.value = 20;
    try sampler.sample(m);
    try std.testing.expectEqual(1, (try sampler.currentBucket()).size());

    currentBucket = try sampler.currentBucket();
    for (currentBucket.series.values()) |serie| {
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
    var m = try Event.initMetric(allocator, "this.is.my.counter");
    defer m.deinit(allocator);
    m.value = 50.0;
    m.type = .Counter;
    m.tags = .empty;

    try sampler.sample(m);
    try std.testing.expectEqual(1, (try sampler.currentBucket()).size());

    var currentBucket = try sampler.currentBucket();
    var iterator = currentBucket.series.iterator();
    if (iterator.next()) |kv| {
        const serie = kv.value_ptr.*;
        assert(serie.value == 50.0);
    }

    m.value = 20;

    try sampler.sample(m);
    currentBucket = try sampler.currentBucket();
    for (currentBucket.series.values()) |serie| {
        assert(serie.value == 70.0);
    }

    sampler.deinit();
}

test "sampler bucketing" {
    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };
    const allocator = std.testing.allocator;

    var sampler = try Sampler.init(allocator, config);
    defer sampler.deinit();

    try std.testing.expectEqual(0, sampler.buckets.size);

    try sampler.sample(.{
        .name = "my_test",
        .value = 10.0,
        .type = .Counter,
        .tags = .empty,
    });
    try std.testing.expectEqual(1, sampler.buckets.size);

    // make sure we change the bucket
    std.time.sleep((sampling_interval+1)*std.time.ns_per_s);
    try sampler.sample(.{
        .name = "my_test",
        .value = 10.0,
        .type = .Counter,
        .tags = .empty,
    });
    try std.testing.expectEqual(2, sampler.buckets.size);

    // not ideal because it's testing for sampling_interval=10 here,
    // but to be fair it will most likely never change.
    try std.testing.expectEqual(1000, Bucket.key(1001));
    try std.testing.expectEqual(1000, Bucket.key(1009));
    try std.testing.expectEqual(1110, Bucket.key(1111));
    try std.testing.expectEqual(1110, Bucket.key(1115));
    try std.testing.expectEqual(1110, Bucket.key(1119));
}

