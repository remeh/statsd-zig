const std = @import("std");
const pb = @import("protobuf/agent_payload.pb.zig");
const protobuf = @import("protobuf");

const Config = @import("config.zig").Config;
const DDSketch = @import("ddsketch.zig").DDSketch;
const Distribution = @import("sampler.zig").Distribution;
const TagsSetUnmanaged = @import("metric.zig").TagsSetUnmanaged;

/// SketchesFromDistributions converts the `dists` to a `SketchPayload`
/// ready to be encoded.
pub fn SketchesFromDistributions(allocator: std.mem.Allocator, config: Config, dists: []Distribution, bucket: u64) !pb.SketchPayload {
    var rv = pb.SketchPayload.init(allocator);
    for (dists) |dist| {
        try rv.sketches.append(try SketchFromDistribution(allocator, config, dist, bucket));
    }
    return rv;
}

fn SketchFromDistribution(allocator: std.mem.Allocator, config: Config, dist: Distribution, bucket: u64) !pb.SketchPayload.Sketch {
    var rv = pb.SketchPayload.Sketch.init(allocator);

    rv.metric = protobuf.ManagedString.managed(dist.metric_name);
    for (dist.tags.tags.items) |tag| {
        try rv.tags.append(protobuf.ManagedString.managed(tag));
    }
    rv.host = protobuf.ManagedString.managed(config.hostname);

    var sk = pb.SketchPayload.Sketch.Dogsketch.init(allocator);
    sk.ts = @intCast(bucket);
    sk.cnt = dist.sketch.count;
    sk.min = dist.sketch.min;
    sk.max = dist.sketch.max;
    sk.avg = dist.sketch.avg;
    sk.sum = dist.sketch.sum;
    var ks = std.ArrayList(i32).init(allocator);
    var ns = std.ArrayList(u32).init(allocator);

    for (dist.sketch.bins.items) |bin| {
        // TODO(remy): because at the end of the day we do this,
        // we could use a better data structure for bins in a DDSketch...
        try ks.append(@intCast(bin.k));
        try ns.append(@intCast(bin.n));
    }
    sk.k = ks;
    sk.n = ns;
    try rv.dogsketches.append(sk);

    // TODO(remy): set to mimic the Agent?
    rv.metadata = pb.Metadata{
        .origin = .{},
    };

    return rv;
}

test "SketchesFromDistributions" {
    const config = Config{
        .hostname = "local",
        .apikey = "abcdef",
        .force_curl = false,
        .max_mem_mb = 20000,
        .uds = false,
    };
    const allocator = std.testing.allocator;

    var tags1: TagsSetUnmanaged = .empty;
    try tags1.appendCopy(allocator, "first");
    try tags1.appendCopy(allocator, "second");
    var tags2: TagsSetUnmanaged = .empty;
    try tags2.appendCopy(allocator, "third");
    try tags2.appendCopy(allocator, "fourth");

    var dists: [2]Distribution = .{
        Distribution{
            .metric_name = "my.dist",
            .tags = tags1,
            .sketch = DDSketch.initDefault(allocator),
        },
        Distribution{
            .metric_name = "my.other.dist",
            .tags = tags2,
            .sketch = DDSketch.initDefault(allocator),
        },
    };
    defer dists[0].deinit(allocator);
    defer dists[1].deinit(allocator);

    const payload = try SketchesFromDistributions(allocator, config, dists[0..], 0);

    try std.testing.expectEqual(2, payload.sketches.items.len);
    try std.testing.expectEqualStrings("my.dist", payload.sketches.items[0].metric.Const);
    try std.testing.expectEqualStrings("first", payload.sketches.items[0].tags.items[0].Const);
    try std.testing.expectEqualStrings("second", payload.sketches.items[0].tags.items[1].Const);
    try std.testing.expectEqualStrings("my.other.dist", payload.sketches.items[1].metric.Const);
    try std.testing.expectEqualStrings("third", payload.sketches.items[1].tags.items[0].Const);
    try std.testing.expectEqualStrings("fourth", payload.sketches.items[1].tags.items[1].Const);

    payload.deinit();
}
