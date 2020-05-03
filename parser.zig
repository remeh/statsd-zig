const std = @import("std");
const assert = @import("std").debug.assert;

const metric = @import("metric.zig");
const Packet = @import("listener.zig").Packet;

pub const ParsingError = error{
    MalformedNameValue,
    MalformedType,
    MalformedTags,
    MalformedPacket,
};

pub const Parser = struct {
    fn split_name_and_value(string: []const u8) ParsingError!metric.Metric {
        var iterator = std.mem.split(string, ":");
        const part: ?[]const u8 = iterator.next();
        var rv: metric.Metric = metric.Metric{
            .name = "",
            .value = 0.0,
            .type = metric.MetricTypeUnknown,
            .tags = undefined,
        };
        var idx: u8 = 0;
        while (part != null) {
            if (idx == 0) {
                rv.name = part.?;
                idx += 1;
            } else if (idx == 1) {
                if (std.fmt.parseFloat(f32, part.?)) |value| {
                    rv.value = value;
                } else |err| { return ParsingError.MalformedNameValue; }
                idx += 1;
            } else {
                return ParsingError.MalformedNameValue;
            }
            part = iterator.next();
        }

        return rv;
    }

    pub fn parse_packet(allocator: *std.mem.Allocator, metric_packet: Packet) !std.ArrayList(metric.Metric) {
        var iterator = std.mem.split(metric_packet.payload[0..metric_packet.len], "\n");
        const part: ?[]const u8 = iterator.next();

        var rv = std.ArrayList(metric.Metric).init(allocator);
        errdefer rv.deinit();

        while (part != null) {
            var m: metric.Metric = try parse_metric(allocator, part.?);
            _ = try rv.append(m);
            part = iterator.next();
        }

        return rv;
    }

    pub fn parse_metric(allocator: *std.mem.Allocator, packet: []const u8) !metric.Metric {
        var iterator = std.mem.split(packet, "|");
        const part: ?[]const u8 = iterator.next();
        var idx: u8 = 0;

        var rv: metric.Metric = metric.Metric {
            .name = undefined,
            .value = 0.0,
            .type = metric.MetricTypeUnknown,
            .tags = undefined,
        };

        while (part != null) {
            switch (idx) {
                0 => {
                    // name and value
                    const nv: metric.Metric = try split_name_and_value(part.?);
                    rv.name = nv.name;
                    rv.value = nv.value;
                },
                1 => {
                    // metric type
                    if (part.?[0] == 'c') {
                        rv.type = metric.MetricTypeCounter;
                    } else if (part.?[0] == 'g') {
                        rv.type = metric.MetricTypeGauge;
                    }
                },
                2 => {
                    // metric tags
                    if (parse_tags(allocator, part.?)) |tags| {
                        rv.tags = tags;
                    } else |err| { return ParsingError.MalformedTags; }
                },
                else => {
                    return ParsingError.MalformedPacket;
                }
            }
            idx += 1;
            part = iterator.next();
        }

        if (idx < 2) {
            return ParsingError.MalformedPacket;
        }

        return rv;
    }

    pub fn parse_tags(allocator: *std.mem.Allocator, buffer: []const u8) anyerror!metric.Tags {
        var rv = metric.Tags.init(allocator);
        errdefer rv.deinit();
        if (buffer[0] != '#') { return ParsingError.MalformedTags; }
        var iterator = std.mem.split(buffer[1..buffer.len], ",");
        const part: ?[]const u8 = iterator.next();
        while (part != null) {
            _ = try rv.append(part.?);
            part = iterator.next();
        }

        var tags = rv.span();
        std.sort.insertionSort([]const u8, tags, lessThanTags);

        return rv;
    }

    fn lessThanTags(l: []const u8, r: []const u8) bool {
        return std.mem.lessThan(u8, l, r);
    }
};

test "split_name_and_value" {
    const packet = "hello:5.0";

    var m = try Parser.split_name_and_value(packet);
    assert(std.mem.eql(u8, m.name, "hello"));
    assert(m.value == 5.0);
}

test "parse_metric" {
    const packet = "hello:5.0|c|#tags:hello";

    var m = try Parser.parse_metric(std.testing.allocator, packet);
    assert(std.mem.eql(u8, m.name, "hello"));
    assert(m.value == 5.0);

    m.tags.deinit();
}

test "parse_tags" {
    const tags: []const u8 = "#my:tag,dev:env,z:value,aaa:aba,aaa:aaa1";
    var rv = try Parser.parse_tags(std.testing.allocator, tags);

    assert(rv.span().len == 5);
    assert(std.mem.eql(u8, rv.span()[0], "aaa:aaa1"));
    assert(std.mem.eql(u8, rv.span()[1], "aaa:aba"));
    assert(std.mem.eql(u8, rv.span()[2], "dev:env"));
    assert(std.mem.eql(u8, rv.span()[3], "my:tag"));
    assert(std.mem.eql(u8, rv.span()[4], "z:value"));

    rv.deinit();
}
