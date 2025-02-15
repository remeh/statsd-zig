const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "statsd",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.linkLibC();
    exe.linkSystemLibrary("curl");

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // run step

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // tests step

    const test_step = b.step("test", "Run unit tests");
    const tests = b.addTest(.{
        .name = "meh unit tests",
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    tests.linkLibC();
    tests.linkSystemLibrary("curl");
    const run_tests = b.addRunArtifact(tests);
    test_step.dependOn(&run_tests.step);
}
