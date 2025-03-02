# statsd-zig

Basic DogStatsD UDP/UDS server supporting gauges, counters and distributions
and sending these metrics to Datadog.

## Getting started

- `libcurl` must be available on the system on macOS
- Build the binary with `zig build` (build with Zig `0.14.0`)
- Set the environment variables `APIKEY` and `HOSTNAME` to configure the daemon

## Socket

Uses `epoll` on Linux, `kqueue` on macOS. 

### With UDP

- Launch the daemon and start sending it counters and gauges on port udp/8125

### With UDS

- Set the environment variable `UDS` to a filepath of the unix socket you want to use
- Sends counters and gauges on this unix socket with a DogStatsD client

## Memory usage

I've developed a custom memory allocator measuring how much memory is allocated
while processing the metrics (see [measure_allocator.zig](https://github.com/remeh/statsd-zig/blob/master/src/measure_allocator.zig)).

Using this allocator, the memory is allocated, used, and forgotten for a while.
When the total amount of allocated memory reaches a certain size, it is completely
freed.

This provides two benefits:

- close to no time spent freeing memory
- you can easily configure the maximum amount of memory the server can use (set `MAX_MEM_MB`, default value 256)

# Author

Rémy 'remeh' Mathieu

# License

This side-project is not endorsed in any way by Datadog.

MIT
