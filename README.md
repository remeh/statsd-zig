# statsd-zig

Basic DogStatsD UDP/UDS server supporting gauges, counters and distributions
and sending these metrics to Datadog.

## Getting started

- Build the binary with `zig build` (build with Zig `0.14.0`)
- Set the environment variables `APIKEY` and `HOSTNAME` to configure the daemon

Uses `epoll` on Linux, `kqueue` on macOS. 

## Socket

### With UDP

- Launch the daemon and start sending counters, gauges and distributions on port udp/8125

### With UDS

- Set the environment variable `UDS` to a filepath of the unix socket you want to use
- Sends counters, gauges and distributions on this unix socket with a DogStatsD client

# Author

Rémy 'remeh' Mathieu

# License

This side-project is not endorsed in any way by Datadog.

MIT
