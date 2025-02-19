const std = @import("std");

pub const MetricType = enum {
    Gauge,
    Counter,
    Unknown,
};

pub const Tags = std.ArrayList([]const u8);

pub const Metric = struct {
    name: []const u8,
    value: f32,
    type: MetricType,
    tags: ?Tags,
};
