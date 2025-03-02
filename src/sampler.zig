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

pub const Distribution = struct {
    metric_name: []const u8,
    tags: TagsSetUnmanaged,
    sketch: DDSketch,

    pub fn deinit(self: *Distribution, allocator: std.mem.Allocator) void {
        self.tags.deinit(allocator);
        self.sketch.deinit();
    }
};

// TODO(remy): comment me
pub const Sampler = struct {
    arena: std.heap.ArenaAllocator,
    config: Config,
    forwarder: Forwarder,

    series: std.AutoHashMapUnmanaged(u64, Serie),
    // TODO(remy): benchmark against std.AutoHashMapUnmanaged
    distributions: std.AutoArrayHashMapUnmanaged(u64, Distribution),

    mutex: std.Thread.Mutex,

    pub fn init(gpa: std.mem.Allocator, config: Config) !Sampler {
        return Sampler{
            .arena = std.heap.ArenaAllocator.init(gpa),
            .config = config,
            .forwarder = try Forwarder.init(gpa, config),
            .series = .empty,
            .distributions = .empty,
            .mutex = std.Thread.Mutex{},
        };
    }

    // TODO(remy): with a LockType parameter on the `sample` function, we can remove
    // the lock usage from the happy path.
    pub fn sample(self: *Sampler, m: metric.Metric) !void {
        const h = Sampler.hash(m);

        if (m.type == .Distribution) {
            return self.sampleDistribution(m, h);
        }

        return self.sampleSerie(m, h);
    }

    pub fn sampleDistribution(self: *Sampler, m: metric.Metric, h: u64) !void {
        self.mutex.lock();
        const k = self.distributions.get(h);
        self.mutex.unlock();

        if (k) |d| {
            // existing
            var newDistribution = d;
            try newDistribution.sketch.insert(@floatCast(m.value));
            self.mutex.lock();
            try self.distributions.put(self.arena.allocator(), h, newDistribution);
            self.mutex.unlock();
            return;
        }

        // not existing

        const name = try self.arena.allocator().alloc(u8, m.name.len);
        std.mem.copyForwards(u8, name, m.name);

        // TODO(remy): could we steal these tags from the metric instead?
        var tags = TagsSetUnmanaged.empty;
        for (m.tags.tags.items) |tag| {
            try tags.appendCopy(self.arena.allocator(), tag);
        }

        const sketch = DDSketch.initDefault(self.arena.allocator());

        self.mutex.lock();
        try self.distributions.put(self.arena.allocator(), h, Distribution{
            .metric_name = name,
            .tags = tags,
            .sketch = sketch,
        });
        self.mutex.unlock();
        return;
    }

    pub fn sampleSerie(self: *Sampler, m: metric.Metric, h: u64) !void {
        self.mutex.lock();
        const k = self.series.get(h);
        self.mutex.unlock();
        if (k) |s| {
            var newSerie = s;
            newSerie.samples += 1;

            switch (s.metric_type) {
                .Gauge => newSerie.value = m.value,
                else => newSerie.value += m.value, // Counter
            }

            self.mutex.lock();
            try self.series.put(self.arena.allocator(), h, newSerie);
            self.mutex.unlock();
            return;
        }

        // not existing, put it in the sampler

        const name = try self.arena.allocator().alloc(u8, m.name.len);
        std.mem.copyForwards(u8, name, m.name);

        // TODO(remy): instead of copying, could we steal these tags from the metric?
        var tags = TagsSetUnmanaged.empty;
        for (m.tags.tags.items) |tag| {
            try tags.appendCopy(self.arena.allocator(), tag);
        }

        self.mutex.lock();
        try self.series.put(self.arena.allocator(), h, Serie{
            .metric_name = name,
            .metric_type = m.type,
            .samples = 1,
            .tags = tags,
            .value = m.value,
        });
        self.mutex.unlock();
    }

    pub fn size(self: *Sampler) usize {
        return self.series.count() + self.distributions.count();
    }

    // TODO(remy): let's not reset the arena on every flush
    // note that if we don't reset the arena on every flush,
    // we'll then have to reset the series & distributions
    // maps in some way (retaining capacity?).
    pub fn flush(self: *Sampler) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.forwarder.flush(self.series, self.distributions);

        self.series = .empty;
        self.distributions = .empty;
        _ = self.arena.reset(.free_all); // TODO(remy): look into retaining capacity
    }

    /// destroy frees all the memory used by the Sampler and the instance itself.
    /// The Sampler instance should not be used anymore after a call to destroy.
    pub fn deinit(self: *Sampler) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.series.deinit(self.arena.allocator());
        self.distributions.deinit(self.arena.allocator());
        self.arena.deinit();
        self.forwarder.deinit();
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

test "sampling hashing" {
    var sampler = try Sampler.init(std.testing.allocator);
    var tags = try Parser.parse_tags(std.testing.allocator, "#my:tag,second:tag");
    const m = metric.Metric{
        .name = "this.is.my.metric",
        .value = 50.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    try Sampler.sample(sampler, m);
    assert(Sampler.size(sampler) == 1);

    try Sampler.sample(sampler, m);
    assert(Sampler.size(sampler) == 1);

    const m2 = metric.Metric{
        .name = "this.is.my.metric",
        .value = 25.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    try Sampler.sample(sampler, m2);
    assert(Sampler.size(sampler) == 1);

    const m3 = metric.Metric{
        .name = "this.is.my.other.metric",
        .value = 25.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    try Sampler.sample(sampler, m3);
    assert(Sampler.size(sampler) == 2);

    var other_tags = try Parser.parse_tags(std.testing.allocator, "#my:tag,second:tag,and:other");

    sampler.deinit();
    tags.deinit();
    other_tags.deinit();
}

test "sampling gauge" {
    var sampler = try Sampler.init(std.testing.allocator);
    var m = metric.Metric{
        .name = "this.is.my.gauge",
        .value = 50.0,
        .type = metric.MetricTypeGauge,
        .tags = undefined,
    };

    try Sampler.sample(sampler, m);
    assert(Sampler.size(sampler) == 1);

    var iterator = sampler.series.iterator();
    if (iterator.next()) |kv| {
        const serie = kv.value_ptr.*;
        assert(serie.value == 50.0);
    }

    m.value = 20;

    try Sampler.sample(sampler, m);
    iterator = sampler.series.iterator();
    if (iterator.next()) |kv| {
        const serie = kv.value_ptr.*;
        assert(serie.value == 20.0);
    }

    sampler.deinit();
}

test "sampling counter" {
    var sampler = try Sampler.init(std.testing.allocator);
    var m = metric.Metric{
        .name = "this.is.my.counter",
        .value = 50.0,
        .type = metric.MetricTypeCounter,
        .tags = undefined,
    };

    try Sampler.sample(sampler, m);
    assert(Sampler.size(sampler) == 1);

    var iterator = sampler.series.iterator();
    if (iterator.next()) |kv| {
        const serie = kv.value_ptr.*;
        assert(serie.value == 50.0);
        assert(serie.samples == 1);
    }

    m.value = 20;

    try Sampler.sample(sampler, m);
    iterator = sampler.series.iterator();
    if (iterator.next()) |kv| {
        const serie = kv.value_ptr.*;
        assert(serie.value == 70.0);
        assert(serie.samples == 2);
    }

    sampler.deinit();
}
