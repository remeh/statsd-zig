const std = @import("std");
const mem = std.mem;
const warn = std.debug.warn;
const fnv1a = std.hash.Fnv1a_64;
const assert = @import("std").debug.assert;

const metric = @import("metric.zig");
const Parser = @import("parser.zig").Parser; // used in tests
const Forwarder = @import("forwarder.zig").Forwarder;

pub const Sample = struct {
    metric_name: []u8,
    metric_type: u8,
    samples: u64,
    value: f32,
};

pub const Sampler = struct {
    map: std.AutoHashMap(u64, Sample),
    allocator: *std.mem.Allocator,
    mutex: std.Mutex,

    pub fn init(allocator: *std.mem.Allocator) !*Sampler {
        var rv = try allocator.create(Sampler);
        rv.map = std.AutoHashMap(u64, Sample).init(allocator);
        rv.allocator = allocator;
        rv.mutex = std.Mutex.init();
        return rv;
    }

    pub fn sample(self: *Sampler, m: metric.Metric) !void {
        const h = Sampler.hash(m);
        var k = self.map.getValue(h);
        if (k) |s| {
            var newSample = Sample{
                .metric_name = s.metric_name,
                .metric_type = s.metric_type,
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
            var held = self.mutex.acquire();
            _ = try self.map.put(h, newSample);
            held.release();
            return;
        }

        // not existing, put it in the sampler
        var name = try self.allocator.alloc(u8, m.name.len);
        std.mem.copy(u8, name, m.name);
        var held = self.mutex.acquire();
        _ = try self.map.put(h, Sample{
            .metric_name = name,
            .metric_type = m.type,
            .samples = 1,
            .value = m.value,
        });
        held.release();
    }

    pub fn size(self: *Sampler) usize {
        return self.map.count();
    }

    pub fn reset(self: *Sampler) void {
        self.map.clear();
    }

    pub fn flush(self: *Sampler) !void {
        var held = self.mutex.acquire();
        try Forwarder.flush(self.allocator, self.map);
        self.map = std.AutoHashMap(u64, Sample).init(self.allocator);
        held.release();
    }

    /// destroy frees all the memory used by the Sampler and the instance itself.
    /// The Sampler instance should not be used anymore after a call to destroy.
    pub fn destroy(self: *Sampler) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            self.allocator.free(kv.*.value.metric_name);
        }
        self.map.deinit();
        self.allocator.destroy(self);
        self.mutex.deinit();
    }

    // TODO(remy): debug method, remove?
    pub fn dump(self: *Sampler) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            const s = kv.*.value;
            warn("{d}: {} ({c}): {d}\n", .{ kv.*.key, s.metric_name, s.metric_type, s.value });
        }
    }

    fn hash(m: metric.Metric) u64 {
        var h = fnv1a.init();
        h.update(m.name);
        var i: usize = 0;
        while (i < m.tags.span().len) {
            h.update(m.tags.span()[i]);
            i += 1;
        }
        return h.final();
    }
};

test "sampling hashing" {
    var sampler = try Sampler.init(std.testing.allocator);
    var tags = try Parser.parse_tags(std.testing.allocator, "#my:tag,second:tag");
    var m = metric.Metric{
        .name = "this.is.my.metric",
        .value = 50.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    try Sampler.sample(sampler, m);
    assert(Sampler.size(sampler) == 1);

    try Sampler.sample(sampler, m);
    assert(Sampler.size(sampler) == 1);

    var m2 = metric.Metric{
        .name = "this.is.my.metric",
        .value = 25.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    try Sampler.sample(sampler, m2);
    assert(Sampler.size(sampler) == 1);

    var m3 = metric.Metric{
        .name = "this.is.my.other.metric",
        .value = 25.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    try Sampler.sample(sampler, m3);
    assert(Sampler.size(sampler) == 2);

    var other_tags = try Parser.parse_tags(std.testing.allocator, "#my:tag,second:tag,and:other");
    var m4 = metric.Metric{ // different because it has other tags
        .name = "this.is.my.other.metric",
        .value = 25.0,
        .type = metric.MetricTypeCounter,
        .tags = tags,
    };

    Sampler.destroy(sampler);
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
        const sample = kv.*.value;
        assert(sample.value == 50.0);
    }

    m.value = 20;

    try Sampler.sample(sampler, m);
    iterator = sampler.map.iterator();
    if (iterator.next()) |kv| {
        const sample = kv.*.value;
        assert(sample.value == 20.0);
    }

    Sampler.destroy(sampler);
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
        const sample = kv.*.value;
        assert(sample.value == 50.0);
        assert(sample.samples == 1);
    }

    m.value = 20;

    try Sampler.sample(sampler, m);
    iterator = sampler.map.iterator();
    if (iterator.next()) |kv| {
        const sample = kv.*.value;
        assert(sample.value == 70.0);
        assert(sample.samples == 2);
    }

    Sampler.destroy(sampler);
}
