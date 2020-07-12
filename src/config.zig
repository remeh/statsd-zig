const std = @import("std");

pub const ConfigError = error{
    MissingApikey,
    MissingHostname,
};

pub const Config = struct {
    hostname: []const u8,
    apikey: []const u8,

    pub fn read() ConfigError!Config {
        var apikey = std.os.getenv("APIKEY");
        if (apikey == null) {
            std.debug.warn("APIKEY should be set!\n", .{});
            return ConfigError.MissingApikey;
        }

        var hostname = std.os.getenv("HOSTNAME");
        if (hostname == null) {
            std.debug.warn("HOSTNAME should be set!\n", .{});
            return ConfigError.MissingHostname;
        }

        return Config{
            .apikey = apikey.?,
            .hostname = hostname.?,
        };
    }
};
