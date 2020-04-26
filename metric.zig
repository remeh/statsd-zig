const std = @import("std");

pub const MetricTypeGauge:   u8 = 'g';
pub const MetricTypeCounter: u8 = 'c';
pub const MetricTypeUnknown: u8 = 0;

pub const Metric = struct {
    name: []const u8,
    value: f32,
    type: u8,
    // TODO(remy): tags
};
