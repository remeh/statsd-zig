const std = @import("std");
const mem = std.mem;
const warn = std.log.warn;
const fnv1a = std.hash.Fnv1a_64;
const assert = std.debug.assert;

const metric = @import("metric.zig");
const Config = @import("config.zig").Config;
const Parser = @import("parser.zig").Parser; // used in tests
const Forwarder = @import("forwarder.zig").Forwarder;
const Transaction = @import("forwarder.zig").Transaction;

pub const Sample = struct {
    metric_name: []u8,
    metric_type: u8,
    tags: metric.Tags,
    samples: u64,
    value: f32,
};

pub const Sampler = struct {
    allocator: std.mem.Allocator,
    config: Config,
    forwarder: Forwarder,
    map: std.AutoHashMap(u64, Sample),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Sampler {
        return Sampler{
            .allocator = allocator,
            .config = config,
            .forwarder = try Forwarder.init(allocator, config),
            .map = std.AutoHashMap(u64, Sample).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn sample(self: *Sampler, m: metric.Metric) !void {
        const h = Sampler.hash(m);
        const k = self.map.get(h);
        if (k) |s| {
            var newSample = Sample{
                .metric_name = s.metric_name,
                .metric_type = s.metric_type,
                .tags = s.tags,
                .samples = s.samples + 1,
                .value = s.value,
            };
            switch (s.metric_type) {
                metric.MetricTypeGauge => {
                    newSample.value = m.value;
                },
                // MetricTypeCounter & MetricTypeUnknown
                else => {
                    newSample.value += m.value;
                },
            }
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.map.put(h, newSample);
            return;
        }

        // not existing, put it in the sampler

        const name = try self.allocator.alloc(u8, m.name.len);
        var tags = metric.Tags.init(self.allocator);
        for (m.tags.items) |tag| {
            const tag_copy = try self.allocator.alloc(u8, tag.len);
            std.mem.copyForwards(u8, tag_copy, tag);
            try tags.append(tag_copy);
        }

        std.mem.copyForwards(u8, name, m.name);
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.map.put(h, Sample{
            .metric_name = name,
            .metric_type = m.type,
            .samples = 1,
            .tags = tags,
            .value = m.value,
        });
    }

    pub fn size(self: *Sampler) usize {
        return self.map.count();
    }

    pub fn flush(self: *Sampler) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.forwarder.flush(self.allocator, self.config, &self.map);

        // release the memory used for all metrics names, tags and reset the map
        var it = self.map.iterator();
        while (it.next()) |kv| {
            self.allocator.free(kv.value_ptr.*.metric_name);
            for (kv.value_ptr.*.tags.items) |tag| {
                self.allocator.free(tag);
            }
            kv.value_ptr.*.tags.deinit();
        }
        self.map.deinit();
        self.map = std.AutoHashMap(u64, Sample).init(self.allocator);
    }

    /// destroy frees all the memory used by the Sampler and the instance itself.
    /// The Sampler instance should not be used anymore after a call to destroy.
    pub fn deinit(self: *Sampler) void {
        // release the memory used for all metrics names
        var it = self.map.iterator();
        while (it.next()) |kv| {
            self.allocator.free(kv.value_ptr.*.metric_name);
            for (kv.value_ptr.*.tags.items) |tag| {
                self.allocator.free(tag);
            }
            kv.value_ptr.*.tags.deinit();
        }
        self.forwarder.deinit();
        self.map.deinit();
    }

    // TODO(remy): debug method, remove?
    pub fn dump(self: *Sampler) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            const s = kv.*.value;
            std.log.debug("{d}: {} ({c}): {d}", .{ kv.*.key, s.metric_name, s.metric_type, s.value });
        }
    }

    fn hash(m: metric.Metric) u64 {
        var h = fnv1a.init();
        h.update(m.name);
        var i: usize = 0;
        while (i < m.tags.items.len) : (i += 1) {
            h.update(m.tags.items[i]);
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

    var iterator = sampler.map.iterator();
    if (iterator.next()) |kv| {
        const sample = kv.value_ptr.*;
        assert(sample.value == 50.0);
    }

    m.value = 20;

    try Sampler.sample(sampler, m);
    iterator = sampler.map.iterator();
    if (iterator.next()) |kv| {
        const sample = kv.value_ptr.*;
        assert(sample.value == 20.0);
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

    var iterator = sampler.map.iterator();
    if (iterator.next()) |kv| {
        const sample = kv.value_ptr.*;
        assert(sample.value == 50.0);
        assert(sample.samples == 1);
    }

    m.value = 20;

    try Sampler.sample(sampler, m);
    iterator = sampler.map.iterator();
    if (iterator.next()) |kv| {
        const sample = kv.value_ptr.*;
        assert(sample.value == 70.0);
        assert(sample.samples == 2);
    }

    sampler.deinit();
}
