const std = @import("std");
const metric = @import("metric.zig");

pub const ParsingError = error{
    MalformedNameValue,
    MalformedType,
    MalformedTags,
    MalformedPacket,
};

pub const Parser = struct {
    pub fn init() Parser {
        return Parser{};
    }

    fn split_name_and_value(string: []const u8) ParsingError!metric.Metric {
        var iterator = std.mem.split(string, ":");
        const part: ?[]const u8 = iterator.next();
        var rv: metric.Metric = metric.Metric{
            .name = "",
            .value = 0.0,
            .type = metric.MetricTypeUnknown,
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

    // TODO(remy): use a pre-allocated array with an int of how many metrics it contains
    pub fn parse_packet(metric_packet: []const u8) !std.ArrayList(metric.Metric) {
        var iterator = std.mem.split(metric_packet, "\n");
        const part: ?[]const u8 = iterator.next();
        var idx: u8 = 0;
        
        var rv = std.ArrayList(metric.Metric).init(std.heap.page_allocator);
        
        while (part != null) {
            var m: metric.Metric = try parse_metric(part.?);
            _ = try rv.append(m);
            part = iterator.next();
        }

        return rv;
    }

    pub fn parse_metric(packet: []const u8) ParsingError!metric.Metric {
        var iterator = std.mem.split(packet, "|");
        const part: ?[]const u8 = iterator.next();
        var idx: u8 = 0;
        
        var rv: metric.Metric = metric.Metric{
          .name = "",
          .value = 0.0,
          .type = metric.MetricTypeUnknown,
        };
        
        while (part != null) {
            switch (idx) {
                0 => {
                    const nv: metric.Metric = try split_name_and_value(part.?);
                    rv.name = nv.name;
                    rv.value = nv.value;
                },
                1 => {
                    // TODO(remy): type
                },
                2 => {
                    // TODO(remy): implement me
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

//        std.debug.warn("{}\n", .{rv});
        
        return rv;
    }
};