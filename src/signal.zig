const std = @import("std");
const builtin = @import("builtin");

/// Signal uses epoll/kqueue to allow for performant blocking waits
/// and thread-safe signals between threads.
pub const Signal = struct {
    notification_fd: i32 = 0,
    poll_fd: i32 = 0,

    pub fn init() !Signal {
        switch (builtin.os.tag) {
            .linux => {
                const eventfd = try std.posix.eventfd(0, std.os.linux.EFD.CLOEXEC | std.os.linux.EFD.NONBLOCK);
                const epfd = std.os.linux.epoll_create();
                var epev = std.os.linux.epoll_event{
                    .events = std.os.linux.EPOLL.IN | std.os.linux.EPOLL.ET,
                    .data = std.os.linux.epoll_data{ .fd = eventfd },
                };
                try std.posix.epoll_ctl(
                    @intCast(epfd),
                    std.os.linux.EPOLL.CTL_ADD,
                    eventfd,
                    &epev,
                );
                return .{
                    .notification_fd = eventfd,
                    .poll_fd = @intCast(epfd),
                };                
            },
            // TODO(remy): kqueue impl
            else => .{
                .notification_d = 0,
                .poll_fd = 0,
            }
        }
    }

    pub fn deinit(self: *Signal) void {
        switch (builtin.os.tag) {
            .linux => {
                std.posix.linux.close(self.notification_fd);
                std.posix.linux.close(self.poll_fd);
            },
            // TODO(remy): kqueue impl
            else => return,
        }
    }

    // TODO(remy): comment me
    // ms_timeout is how many millisecond max the wait call will wait.
    pub fn wait(self: Signal, ms_timeout: u32) void {
        switch (builtin.os.tag) {
            .linux => {
                var events: [10]std.os.linux.epoll_event = undefined;
                _ = std.os.linux.epoll_wait(@intCast(self.poll_fd), events[0..], 10, @intCast(ms_timeout));
            },
            else => {
                // TODO(remy): kqueue impl
                std.time.nanosleep(0, 10000);
            }
        }
    }

    pub fn emit(self: Signal) !void {
        switch (builtin.os.tag) {
            .linux => {
                const v: usize = 1;
                _ = try std.posix.write(self.notification_fd, std.mem.asBytes(&v));
            },
            else => {}, // TODO(remy): kqueue
        }
    }
};
