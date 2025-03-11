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
            .netbsd, .openbsd, .macos => {
                const kq = try std.posix.kqueue();
                return .{
                    .poll_fd = kq,
                    .notification_fd = 0,
                };
            },
            else => return .{
                .notification_fd = 0,
                .poll_fd = 0,
            },
        }
    }

    pub fn deinit(self: *Signal) void {
        switch (builtin.os.tag) {
            .linux => {
                std.posix.close(self.notification_fd);
                std.posix.close(self.poll_fd);
            },
            .netbsd, .openbsd, .macos => {
                std.posix.close(self.poll_fd);
            },
            else => return,
        }
    }

    // TODO(remy): comment me
    // ms_timeout is how many millisecond max the wait call will wait.
    pub fn wait(self: Signal, ms_timeout: u32) !void {
        switch (builtin.os.tag) {
            .linux => {
                var events: [10]std.os.linux.epoll_event = undefined;
                _ = std.os.linux.epoll_wait(@intCast(self.poll_fd), events[0..], 10, @intCast(ms_timeout));
            },
            .netbsd, .openbsd, .macos => {
                var events: [10]std.posix.Kevent = undefined;
                const sec = ms_timeout / 1000;
                const nsec = (ms_timeout % 1000) * 1000000;
                _ = try std.posix.kevent(self.poll_fd, &.{}, &events, &.{ .sec = sec, .nsec = nsec });
            },
            else => {
                std.time.sleep(100000);
            },
        }
    }

    pub fn emit(self: Signal) !void {
        switch (builtin.os.tag) {
            .linux => {
                const v: usize = 1;
                _ = try std.posix.write(self.notification_fd, std.mem.asBytes(&v));
            },
            .netbsd, .openbsd, .macos => {
                _ = try std.posix.kevent(self.poll_fd, &.{std.posix.Kevent{
                    .ident = @intCast(self.poll_fd),
                    .filter = std.posix.system.EVFILT.USER,
                    .flags = std.posix.system.EV.ADD | std.posix.system.EV.CLEAR,
                    .fflags = std.posix.system.NOTE.TRIGGER,
                    .data = 0,
                    .udata = 0,
                }}, &.{}, null);
            },
            else => {},
        }
    }
};
