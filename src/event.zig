const std = @import("std");

pub const EventType = enum {
    Counter,
    Distribution,
    Gauge,
    Unknown,
};

pub const EventErrors = error {
    UnsupportedType,
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

    /// copy returns a TagsSetUnmanaged with all internal tags copied.
    pub fn copy(self: *const TagsSetUnmanaged, allocator: std.mem.Allocator) !TagsSetUnmanaged {
        var c: TagsSetUnmanaged = .empty;
        for (self.tags.items) |item| {
            try c.append(allocator, item);
        }
        return c;
    }
};

// TODO(remy): comment
// TODO(remy): unit test
pub const Event = struct {
    name: []const u8, // owned
    value: f32,
    type: EventType,
    tags: TagsSetUnmanaged,
    // TODO(remy): timestamp

    /// initMetric creates a metric, copying the name using the given allocator.
    pub fn initMetric(allocator: std.mem.Allocator, name: []const u8) !Event {
        const name_copy = try allocator.alloc(u8, name.len);
        std.mem.copyForwards(u8, name_copy, name);
        return Event{
            .name = name_copy,
            .value = 0,
            .tags = .empty,
            .type = .Unknown,
        };
    }

    pub fn deinit(self: *Event, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.tags.deinit(allocator);
    }
};
