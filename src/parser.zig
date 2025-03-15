const std = @import("std");
const assert = @import("std").debug.assert;

const Event = @import("event.zig").Event;
const EventType = @import("event.zig").EventType;
const Packet = @import("listener.zig").Packet;
const TagsSetUnmanaged = @import("event.zig").TagsSetUnmanaged;

pub const ParsingError = error{
    MalformedNameValue,
    MalformedType,
    MalformedTags,
    MalformedPacket,
};

pub const Parser = struct {
    fn split_name_and_value(allocator: std.mem.Allocator, string: []const u8) !Event {
        var iterator = std.mem.splitSequence(u8, string, ":");
        var part: ?[]const u8 = iterator.next();
        var rv: Event = undefined;
        var idx: u8 = 0;
        while (part != null) {
            if (idx == 0) {
                rv = try Event.initMetric(allocator, part.?);
                errdefer rv.deinit();
                idx += 1;
            } else if (idx == 1) {
                if (std.fmt.parseFloat(f32, part.?)) |value| {
                    rv.value = value;
                } else |_| {
                    return ParsingError.MalformedNameValue;
                }
                idx += 1;
            } else {
                return ParsingError.MalformedNameValue;
            }
            part = iterator.next();
        }

        return rv;
    }

    // TODO(remy): do not use an ArrayList but something fixed to avoid
    // constant reallocation.
    pub fn parse_packet(allocator: std.mem.Allocator, metric_packet: Packet) !std.ArrayList(Event) {
        var iterator = std.mem.splitSequence(u8, metric_packet.payload[0..metric_packet.len], "\n");
        var part: ?[]const u8 = iterator.next();

        var rv = std.ArrayList(Event).init(allocator);
        errdefer rv.deinit();

        while (part != null) {
            if (part.?.len == 0) {
                part = iterator.next();
                continue;
            }
            const m: Event = try parse_metric(allocator, part.?);
            try rv.append(m);
            part = iterator.next();
        }

        return rv;
    }

    pub fn parse_metric(allocator: std.mem.Allocator, packet: []const u8) !Event {
        var iterator = std.mem.splitSequence(u8, packet, "|");
        var part: ?[]const u8 = iterator.next();
        var rv: Event = undefined;

        var idx: u8 = 0;
        while (part != null) {
            switch (idx) {
                0 => {
                    // name and value
                    rv = try split_name_and_value(allocator, part.?);
                },
                1 => {
                    rv.type = switch (part.?[0]) {
                        'c' => .Counter,
                        'd' => .Distribution,
                        'g' => .Gauge,
                        else => .Unknown,
                    };
                },
                2 => {
                    // metric tags
                    if (parse_tags(allocator, part.?)) |tags| {
                        rv.tags = tags;
                    } else |_| {
                        return ParsingError.MalformedTags;
                    }
                },
                else => {
                    return ParsingError.MalformedPacket;
                },
            }

            idx += 1;
            part = iterator.next();
        }

        if (idx < 2) {
            return ParsingError.MalformedPacket;
        }

        return rv;
    }

    pub fn parse_tags(allocator: std.mem.Allocator, buffer: []const u8) !TagsSetUnmanaged {
        var rv: TagsSetUnmanaged = .empty;
        errdefer rv.deinit(allocator);

        if (buffer[0] != '#') {
            return ParsingError.MalformedTags;
        }
        var iterator = std.mem.splitSequence(u8, buffer[1..buffer.len], ",");
        while (iterator.next()) |part| {
            try rv.append(allocator, part);
        }

        std.sort.insertion([]const u8, rv.tags.items, {}, lessThanTags);
        return rv;
    }

    fn lessThanTags(_: void, l: []const u8, r: []const u8) bool {
        return std.mem.lessThan(u8, l, r);
    }
};

test "split_name_and_value" {
    const packet = "hello:5.0";
    const allocator = std.testing.allocator;

    var m = try Parser.split_name_and_value(allocator, packet);
    assert(std.mem.eql(u8, m.name, "hello"));
    assert(m.value == 5.0);
    m.deinit(allocator);
}

test "parse_metric" {
    const allocator = std.testing.allocator;
    var buf = std.ArrayList(u8).init(allocator);
    try buf.appendSlice("hello:5.0|c|#tags:hello");
    defer buf.deinit();
    const packet = buf.items;

    var m = try Parser.parse_metric(std.testing.allocator, packet);
    defer m.deinit(allocator);
    assert(std.mem.eql(u8, m.name, "hello"));
    assert(m.value == 5.0);

    // here we're modifying the packet, and validating
    // that the metric data has not changed, i.e., is not
    // relying on the packet buffer but on its own.
    buf.items[0] = 'a';
    assert(std.mem.eql(u8, m.name, "hello"));
}

test "parse_tags" {
    const tags: []const u8 = "#my:tag,dev:env,z:value,aaa:aba,aaa:aaa1";
    const allocator = std.testing.allocator;
    var rv = try Parser.parse_tags(allocator, tags);

    assert(rv.tags.items.len == 5);
    assert(std.mem.eql(u8, rv.tags.items[0], "aaa:aaa1"));
    assert(std.mem.eql(u8, rv.tags.items[1], "aaa:aba"));
    assert(std.mem.eql(u8, rv.tags.items[2], "dev:env"));
    assert(std.mem.eql(u8, rv.tags.items[3], "my:tag"));
    assert(std.mem.eql(u8, rv.tags.items[4], "z:value"));

    rv.deinit(allocator);
}
