const std = @import("std");

// This is a port of the Go package datadog-agent/pkg/quantile
// https://github.com/DataDog/datadog-agent/tree/7.40.x/pkg/quantile
// It is also inspired from this Rust port in vector/ddsketch.rs
// https://github.com/vectordotdev/vector/blob/v0.45/lib/vector-core/src/metrics/ddsketch.rs

const max_bin_width: u16 = std.math.maxInt(u16);
const uv_inf: i16 = (1 << 15) - 1;
const uv_neginf: i16 = -uv_inf;
const max_key: i16 = uv_inf - 1;

const default_bin_limit: u16 = 4096;
const default_eps: f64 = 1.0 / 128.0;
const default_min: f64 = 1e-9;

const default_config = Config.init(default_eps, default_min, default_bin_limit);

//
// Bin
// ------------------------------

const Bin = struct {
    /// bin index
    k: i16,

    /// number of samples put in the bin.
    n: u16,

    pub fn increment(self: *Bin, by: u32) u32 {
        const next = by + self.n;

        if (next > max_bin_width) {
            self.n = max_bin_width;
            return next - max_bin_width;
        }

        self.n = @intCast(next);
        return 0;
    }
};

//
// Config
// ------------------------------

// The same way it has been done in the Rust one, I removed the norm.max/norm.emin
// which seem to only be used for creation.
// This one should be complete for now, compared to what's coming from the
// Go implementation.
// TODO(remy): comment
// TODO(remy): unit test
const Config = struct {
    bin_limit: u16,
    gamma: struct {
        v: f64,
        ln: f64,
    },
    norm: struct {
        min: f64,
        bias: i32,
    },

    pub fn init(eps: f64, min_value: f64, bin_limit: u16) Config {
        var rv = Config{
            .bin_limit = switch (bin_limit) {
                0 => default_bin_limit,
                else => bin_limit,
            },
            .gamma = undefined,
            .norm = undefined,
        };

        rv.refresh(eps, min_value, rv.bin_limit);
        return rv;
    }

    pub fn pow_gamma(self: Config, y: f64) f64 {
        return std.math.pow(f64, self.gamma.v, y);
    }

    pub fn log_gamma(self: Config, v: f64) f64 {
        return std.math.log(f64, std.math.e, v) / self.gamma.ln;
    }

    pub fn lower_bound(self: Config, k: i16) f64 {
        if (k < 0) {
            return -self.lower_bound(-k);
        }
        if (k == uv_inf or k == -uv_neginf) {
            return std.math.inf(f64);
        }
        if (k == 0) {
            return 0.0;
        }

        const exp = @as(f64, @floatFromInt(k - self.norm.bias));
        return self.pow_gamma(exp);
    }

    pub fn refresh(self: *Config, eps: f64, min_value: f64, bin_limit: u16) void {
        std.debug.assert(eps < 1.0); // eps must be between 0.0 and 1.0
        std.debug.assert(bin_limit > 0); // bin limit must be greater than 0

        // gamma
        var mut_eps = eps;
        if (mut_eps == 0.0) {
            mut_eps = default_eps;
        }

        mut_eps *= 2.0;
        self.gamma = .{
            .v = 1.0 + mut_eps,
            .ln = std.math.log1p(mut_eps),
        };

        var mut_min_value = min_value;
        if (mut_min_value == 0.0) {
            mut_min_value = default_min;
        }

        // norm
        const norm_emin: i32 = @intFromFloat(std.math.floor(self.log_gamma(mut_min_value)));
        self.norm.bias = -norm_emin + 1;

        const norm_min = self.lower_bound(1.0);
        std.debug.assert(norm_min <= mut_min_value);
        self.norm.min = norm_min;
    }

    /// key returns a value k such that:
    ///   γ^k <= v < γ^(k+1)
    pub fn key(self: *Config, v: f64) i16 {
        if (v < 0.0) {
            return -self.key(-v);
        }

        if (v == 0.0 or
            (v > 0.0 and v < self.norm.min) or
            (v < 0.0 and v > -self.norm.min))
        {
            return 0;
        }

        const l_gamma: f64 = self.log_gamma(v);
        const i: i32 = @as(i16, @intFromFloat(@round(l_gamma))) + self.norm.bias;

        if (i > max_key) {
            return uv_inf;
        }
        if (i < 1) {
            return 1;
        }

        return @intCast(i);
    }
};

//
// DDSketch
// ------------------------------

pub const DDSketch = struct {
    allocator: std.mem.Allocator,
    config: Config,

    /// The bins within the sketch.
    bins: std.ArrayList(Bin),

    /// The number of observations within the sketch.
    count: u32,

    /// The minimum value of all observations within the sketch.
    min: f64,

    /// The maximum value of all observations within the sketch.
    max: f64,

    /// The sum of all observations within the sketch.
    sum: f64,

    /// The average value of all observations within the sketch.
    avg: f64,

    pub fn initDefault(allocator: std.mem.Allocator) DDSketch {
        var rv = DDSketch{
            .allocator = allocator,
            .config = default_config,
            .bins = std.ArrayList(Bin).init(allocator),
            .count = undefined,
            .min = undefined,
            .max = undefined,
            .avg = undefined,
            .sum = undefined,
        };

        rv.clear();
        return rv;
    }

    pub fn deinit(self: *DDSketch) void {
        self.bins.deinit();
    }

    pub fn clear(self: *DDSketch) void {
        self.count = 0;
        self.min = std.math.floatMax(f64);
        self.max = std.math.floatMin(f64);
        self.avg = 0.0;
        self.sum = 0.0;
        self.bins.clearAndFree();
    }

    fn adjust_stats(self: *DDSketch, v: f64, n: u32) void {
        if (v < self.min) {
            self.min = v;
        }

        if (v > self.max) {
            self.max = v;
        }

        self.count += n;
        self.sum += v * @as(f64, @floatFromInt(n));

        if (n == 1) {
            self.avg += (v - self.avg) / @as(f64, @floatFromInt(self.count));
        } else {
            self.avg = self.avg + (v - self.avg) * @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(self.count));
        }
    }

    // TODO(remy): once in the statsd server, do I need this implemention? remove if not?
    pub fn insert_keys(self: *DDSketch, keys: []i16) !void {
        std.sort.pdq(i16, keys, {}, std.sort.desc(i16));

        var tmp = std.ArrayList(Bin).init(self.allocator);

        var sIdx: usize = 0;
        var keyIdx: usize = 0;

        while (sIdx < self.bins.items.len and keyIdx < keys.len) {
            const bin = self.bins.items[sIdx];
            const vk = keys[keyIdx];

            if (bin.k < vk) {
                try tmp.append(bin);
            } else if (bin.k > vk) {
                const kn = buf_count_leading_equal(keys, keyIdx);
                try append_safe(&tmp, vk, kn);
                keyIdx += kn;
            } else {
                const kn = buf_count_leading_equal(keys, keyIdx);
                try append_safe(&tmp, bin.k, bin.n + kn);
                sIdx += 1;
                keyIdx += kn;
            }
        }

        try tmp.appendSlice(self.bins.items[sIdx..]);

        while (keyIdx < keys.len) {
            const kn = buf_count_leading_equal(keys, keyIdx);
            try append_safe(&tmp, keys[keyIdx], kn);
            keyIdx += kn;
        }

        try trim_left(self.allocator, &tmp, self.config.bin_limit);

        self.bins.deinit();
        self.bins = tmp;
    }

    pub fn insert(self: *DDSketch, v: f64) !void {
        self.adjust_stats(v, 1);

        const key = self.config.key(v);

        var insert_idx: i16 = 0;

        var bin_idx: i16 = 0;
        for (self.bins.items) |*bin| {
            if (bin.k == key) {
                if (bin.n < max_bin_width) {
                    bin.n += 1; // TODO(remy): should this be modifying the object in the list?
                    return;
                } else {
                    insert_idx = bin_idx;
                    break;
                }
            }

            if (bin.k > key) {
                insert_idx = bin_idx;
                break;
            }

            bin_idx += 1;
        }

        if (bin_idx == insert_idx) {
            try self.bins.insert(@intCast(bin_idx), Bin{ .k = key, .n = 1 });
        } else {
            try self.bins.append(Bin{ .k = key, .n = 1 });
        }

        try trim_left(self.allocator, &self.bins, self.config.bin_limit);
    }

    // TODO(remy): once in the statsd server, do I need this implementation? remove if not
    pub fn insert_many(self: *DDSketch, values: []const f64) !void {
        var keys = try std.ArrayList(i16).initCapacity(self.allocator, values.len);
        for (values) |value| {
            self.adjust_stats(value, 1);
            keys.appendAssumeCapacity(self.config.key(value));
        }
        try self.insert_keys(keys.items);
        keys.deinit();
    }

    pub fn merge(self: *DDSketch, other: DDSketch) !void {
        // merge the basic stats
        self.count += other.count;
        if (other.max > self.max) {
            self.max = other.max;
        }
        if (other.min < self.min) {
            self.min = other.min;
        }

        self.sum += other.sum;
        self.avg = self.avg + (other.avg - self.avg) * @as(f64, @floatFromInt(other.count)) / @as(f64, @floatFromInt(self.count));

        // merge the bins
        // can't use a StackFallbackAllocator here since this
        // becomes the final ArrayList
        var merged = std.ArrayList(Bin).init(self.allocator);

        var bins_idx: usize = 0;
        for (other.bins.items) |other_bin| {
            const start = bins_idx;

            while (bins_idx < self.bins.items.len and self.bins.items[bins_idx].k < other_bin.k) {
                bins_idx += 1;
            }
            try merged.appendSlice(self.bins.items[start..bins_idx]);

            if (bins_idx >= self.bins.items.len or self.bins.items[bins_idx].k > other_bin.k) {
                try merged.append(other_bin);
            } else if (self.bins.items[bins_idx].k == other_bin.k) {
                const n = other_bin.n + self.bins.items[bins_idx].n;
                try append_safe(&merged, other_bin.k, n);
                bins_idx += 1;
            }
        }

        try merged.appendSlice(self.bins.items[bins_idx..]);
        try trim_left(self.allocator, &merged, self.config.bin_limit);

        self.bins.deinit();
        self.bins = merged;
    }

    pub fn quantile(self: DDSketch, q: f64) f64 {
        if (self.count == 0) {
            return 0.0;
        } else if (q <= 0.0) {
            return self.min;
        } else if (q >= 1.0) {
            return self.max;
        }

        var n: f64 = 0.0;
        const wanted_rank: f64 = rank(self.count, q);

        var i: usize = 0;
        for (self.bins.items) |bin| {
            n += @floatFromInt(bin.n);

            if (n <= wanted_rank) {
                i += 1;
                continue;
            }

            const weight: f64 = (n - wanted_rank) / @as(f64, @floatFromInt(bin.n));
            var v_low: f64 = self.config.lower_bound(bin.k);
            var v_high: f64 = v_low * self.config.gamma.v;

            if (i == self.bins.items.len) {
                v_high = self.max;
            } else if (i == 0) {
                v_low = self.min;
            }

            return (v_low * weight + v_high * (1.0 - weight));
        }

        return std.math.nan(f64);
    }
};

inline fn rank(count: u32, q: f64) f64 {
    return @round(q * @as(f64, @floatFromInt(count - 1)));
}

fn trim_left(allocator: std.mem.Allocator, bins: *std.ArrayList(Bin), max_bucket_cap: u16) !void {
    if (max_bucket_cap == 0 or bins.items.len < max_bucket_cap) {
        return;
    }

    const n_remove: usize = bins.items.len - max_bucket_cap;
    var missing: u32 = 0;
    var fallback_allocator = std.heap.stackFallback(4, allocator);
    var overflow = std.ArrayList(Bin).init(fallback_allocator.get());
    defer overflow.deinit();

    var i: usize = 0;
    while (i < n_remove) : (i += 1) {
        missing += bins.items[i].n;

        if (missing > max_bin_width) {
            try overflow.append(Bin{
                .k = bins.items[i].k,
                .n = max_bin_width,
            });

            missing -= max_bin_width;
        }
    }

    var bin_remove = bins.items[n_remove];
    missing = bin_remove.increment(missing);
    if (missing > 0) {
        try append_safe(&overflow, bin_remove.k, missing);
    }

    bins.clearRetainingCapacity();

    // copy(bins, overflow)
    bins.replaceRangeAssumeCapacity(0, overflow.items.len, overflow.items);

    // copy(bins[len(overflow):], bins[n_remove:])
    bins.replaceRangeAssumeCapacity(overflow.items.len, bins.items.len - n_remove, bins.items[n_remove..]);

    // return bins[:max_bucket_cap+len(overflow)]
    bins.shrinkAndFree(max_bucket_cap + overflow.items.len);
}

fn buf_count_leading_equal(a: []const i16, start: usize) usize {
    if (start == a.len - 1) {
        return 1;
    }

    var i = start;
    while (i < a.len and a[i] == a[start]) {
        i += 1;
    }

    return i - start;
}

/// appendSafe appends 1 or more bins with the given key safely handing overflow by
/// inserting multiple buckets when needed.
fn append_safe(bins: *std.ArrayList(Bin), k: i16, n: usize) !void {
    if (n <= max_bin_width) {
        const bin = Bin{ .k = k, .n = @intCast(n) };
        try bins.append(bin);
        return;
    }

    const r: u16 = @intCast(n % max_bin_width);
    if (r != 0) {
        try bins.append(Bin{ .k = k, .n = r });
    }

    var i: usize = 0;
    while (i < n / max_bin_width) : (i += 1) {
        try bins.append(Bin{ .k = k, .n = max_bin_width });
    }
}

test "ddsketch new config" {
    var config = Config.init(0, 0, 0);
    try std.testing.expect(config.key(config.lower_bound(1)) == 1);
}

test "ddsketch insert & clear" {
    const allocator = std.testing.allocator;

    var sketch = DDSketch.initDefault(allocator);
    defer sketch.deinit();

    try std.testing.expectEqual(sketch.count, 0);
    try std.testing.expectEqual(sketch.min, std.math.floatMax(f64));
    try std.testing.expectEqual(sketch.max, std.math.floatMin(f64));
    try std.testing.expectEqual(sketch.sum, 0.0);
    try std.testing.expectEqual(sketch.avg, 0.0);
    try sketch.insert(3.15);
    try std.testing.expectEqual(sketch.count, 1);
    try std.testing.expectEqual(sketch.min, 3.15);
    try std.testing.expectEqual(sketch.max, 3.15);
    try std.testing.expectEqual(sketch.sum, 3.15);
    try std.testing.expectEqual(sketch.avg, 3.15);

    try sketch.insert(2.28);
    try std.testing.expectEqual(sketch.count, 2);
    try std.testing.expectEqual(sketch.min, 2.28);
    try std.testing.expectEqual(sketch.max, 3.15);
    try std.testing.expectEqual(sketch.sum, 5.43);
    try std.testing.expectEqual(sketch.avg, 2.715);

    sketch.clear();
    try std.testing.expectEqual(sketch.count, 0);
    try std.testing.expectEqual(sketch.min, std.math.floatMax(f64));
    try std.testing.expectEqual(sketch.max, std.math.floatMin(f64));
    try std.testing.expectEqual(sketch.sum, 0.0);
    try std.testing.expectEqual(sketch.avg, 0.0);
}

test "rank" {
    const count = 101;
    var p: f64 = 0.0;
    while (p <= 100.0) : (p += 1.0) {
        const q: f64 = p / 100.0;
        const r = rank(count, q);
        try std.testing.expectEqual(r, p);
    }
}

test "negative to positive" {
    const allocator = std.testing.allocator;

    // TODO(remy): test example from Rust version

    const start: f64 = -1.0;
    const end: f64 = 1.0;
    const delta: f64 = 0.01;

    var sketch = DDSketch.initDefault(allocator);
    defer sketch.deinit();

    var v = start;
    while (v <= end) {
        try sketch.insert(v);
        v += delta;
    }

    const min = sketch.quantile(0.0);
    const median = sketch.quantile(0.5);
    const p90 = sketch.quantile(0.9);
    const max = sketch.quantile(1.0);

    try std.testing.expectEqual(start, min);
    try std.testing.expect(median == 0.0);
    try std.testing.expectApproxEqAbs(p90, 0.8, 0.01);
    try std.testing.expectApproxEqAbs(max, 1.0, 0.01);
}

test "merge" {
    const allocator = std.testing.allocator;
    var all_values = DDSketch.initDefault(allocator);
    defer all_values.deinit();
    var odd_values = DDSketch.initDefault(allocator);
    defer odd_values.deinit();
    var even_values = DDSketch.initDefault(allocator);
    defer even_values.deinit();
    var all_values_many = DDSketch.initDefault(allocator);
    defer all_values_many.deinit();

    var values = std.ArrayList(f64).init(allocator);
    defer values.deinit();

    var i: i16 = -50;
    while (i <= 50) : (i += 1) {
        const fv: f64 = @floatFromInt(i);

        try all_values.insert(fv);

        if (i & 1 == 0) {
            try odd_values.insert(fv);
        } else {
            try even_values.insert(fv);
        }

        try values.append(fv);
    }

    try all_values_many.insert_many(values.items);

    const odd_values_count = odd_values.bins.items.len;
    const even_values_count = even_values.bins.items.len;

    try odd_values.merge(even_values);
    const merged_values = odd_values; // alias for unit test clarity

    // Number of bins should be equal to the number of values we inserted.
    try std.testing.expectEqual(all_values.count, values.items.len);
    try std.testing.expectEqual(merged_values.bins.items.len, odd_values_count + even_values_count);

    // Values at both ends of the quantile range should be equal.
    const low_end = all_values.quantile(0.01);
    const high_end = all_values.quantile(0.99);
    try std.testing.expectEqual(high_end, -low_end);

    const target_bin_count = all_values.bins.items.len;

    const list: [3]DDSketch = .{ all_values, all_values_many, merged_values };

    for (list) |sketch| {
        try std.testing.expectEqual(sketch.quantile(0.5), 0.0);
        try std.testing.expectEqual(sketch.quantile(0.0), -50.0);
        try std.testing.expectEqual(sketch.quantile(1.0), 50.0);

        for (0..50) |p| {
            const q: f64 = @as(f64, @floatFromInt(p)) / 100.0;
            const positive = sketch.quantile(q + 0.5);
            const negative = -sketch.quantile(0.5 - q);
            try std.testing.expect(@abs(positive - negative) <= 1.0e-6);
        }
        try std.testing.expectEqual(target_bin_count, sketch.bins.items.len);
    }
}
