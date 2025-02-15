const std = @import("std");

pub const ConfigError = error{
    MissingApikey,
    MissingHostname,
    MalformedMaxMemMB,
};

pub const Config = struct {
    hostname: []const u8,
    apikey: []const u8,
    max_mem_mb: u32,
    uds: bool,

    pub fn read() ConfigError!Config {
        const apikey = std.posix.getenv("APIKEY");
        if (apikey == null) {
            std.log.err("APIKEY should be set!", .{});
            return ConfigError.MissingApikey;
        }

        const hostname = std.posix.getenv("HOSTNAME");
        if (hostname == null) {
            std.log.err("HOSTNAME should be set!", .{});
            return ConfigError.MissingHostname;
        }

        var max_mem_value: u32 = 256;
        if (std.posix.getenv("MAX_MEM_MB")) |value| {
            const parsed_value = std.fmt.parseInt(u32, value, 10) catch {
                return ConfigError.MalformedMaxMemMB;
            };
            max_mem_value = parsed_value;
        }

        var uds: bool = false;
        if (std.posix.getenv("UDS") != null) {
            uds = true;
        }

        return Config{
            .apikey = apikey.?,
            .hostname = hostname.?,
            .max_mem_mb = max_mem_value,
            .uds = uds,
        };
    }
};
