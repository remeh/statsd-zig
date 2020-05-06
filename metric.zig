const std = @import("std");

pub const MetricTypeGauge: u8 = 'g';
pub const MetricTypeCounter: u8 = 'c';
pub const MetricTypeUnknown: u8 = 0;

pub const Tags = std.ArrayList([]const u8);

pub const Metric = struct {
    name: []const u8,
    value: f32,
    type: u8,
    // 64 tags maximum for nw
    tags: Tags,

    // TODO(remy): init / destroy?
};
