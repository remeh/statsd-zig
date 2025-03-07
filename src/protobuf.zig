const std = @import("std");
const pb = @import("protobuf/agent_payload.pb.zig");
const protobuf = @import("protobuf");

const Config = @import("config.zig").Config;
const Distribution = @import("sampler.zig").Distribution;

// TODO(remy): comment me
// TODO(remy): unit test
pub fn SketchesFromDistributions(allocator: std.mem.Allocator, config: Config, dists: []Distribution, bucket: u64) !pb.SketchPayload {
    var rv = pb.SketchPayload.init(allocator);
    for (dists) |dist| {
        try rv.sketches.append(try SketchFromDistribution(allocator, config, dist, bucket));
    }
    return rv;
}

// TODO(remy): comment me
// TODO(remy): unit test
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
        // TODO(remy): because we do this with it in the end,
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
