const std = @import("std");

pub const MetricType = enum {
    Counter,
    Distribution,
    Gauge,
    Unknown,
};

/// TagsSetUnmanaged is a list of tags, always allocating the memory when
/// adding a tag.
pub const TagsSetUnmanaged = struct {
    tags: std.ArrayListUnmanaged([]const u8),

    pub const empty = TagsSetUnmanaged{
        .tags = .empty,
    };

    pub fn deinit(self: *TagsSetUnmanaged, allocator: std.mem.Allocator) void {
        for (self.tags.items) |tag| {
            allocator.free(tag);
        }
        self.tags.deinit(allocator);
    }

    /// append copies the tag into the current tags set.
    pub fn append(self: *TagsSetUnmanaged, allocator: std.mem.Allocator, tag: []const u8) !void {
        const t = try allocator.alloc(u8, tag.len);
        std.mem.copyForwards(u8, t, tag);
        try self.tags.append(allocator, t);
    }
};

// TODO(remy): comment
// TODO(remy): unit test
// TODO(remy): this should be turned into something with a more generic name (Event?)
//             if we start considering the transform implementation.
pub const Metric = struct {
    allocator: std.mem.Allocator,
    name: []const u8, // owned
    value: f32,
    type: MetricType,
    tags: TagsSetUnmanaged,
    // TODO(remy): timestamp

    /// init creates a metric, copying the name using the given allocator.
    pub fn init(allocator: std.mem.Allocator, name: []const u8) !Metric {
        const name_copy = try allocator.alloc(u8, name.len);
        std.mem.copyForwards(u8, name_copy, name);
        return Metric{
            .allocator = allocator,
            .name = name_copy,
            .value = 0,
            .tags = .empty,
            .type = .Unknown,
        };
    }

    pub fn deinit(self: *Metric) void {
        self.allocator.free(self.name);
        self.tags.deinit(self.allocator);
    }
};
