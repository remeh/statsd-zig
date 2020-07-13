# statsd-zig

Basic DogStatsD implementation supporting gauges and counters and sending these
metrics to Datadog.

## Getting started

- `libcurl` must be available on the system
- Build the binary with `zig build`. You will need a recent master version of the
toolchain (>= zig master 2020-07-12)
- Set the environment variables `APIKEY` and `HOSTNAME` to configure the daemon
- Launch the daemon and start sending it counters and gauges on port udp/8125

## Memory usage

Thanks to Zig not having a default memory allocator, I control every byte of
memory allocated in this daemon (even the memory allocated by the standard library).

The pattern I've used let me free all the memory allocated for metrics processing
once it has reached a certain amount, thus, it let me have the total memory
capped to a configurable maximum (set `MAX_MEM_MB`, default value 256).

# Author

Rémy 'remeh' Mathieu

# License

This side-project is not endorsed in anyway by Datadog.

MIT
