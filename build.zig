const std = @import("std");
const protobuf = @import("protobuf");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "statsd",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // protobuf dep
    // ------------

    const protobuf_dep = b.dependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("protobuf", protobuf_dep.module("protobuf"));

    exe.linkLibC();
    // TODO(remy): do not build with libcurl on linux where it's not used anymore
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
        .name = "unit tests",
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
    });

    tests.linkLibC();

    // TODO(remy): do not build with libcurl on linux where it's not used anymore
    tests.linkSystemLibrary("curl");
    const run_tests = b.addRunArtifact(tests);
    test_step.dependOn(&run_tests.step);

    // gen proto tasks
    //--------------

    const gen_proto = b.step("gen-proto", "generates zig files from the protobuf def");
    const protoc_step = protobuf.RunProtocStep.create(b, protobuf_dep.builder, target, .{
        .destination_directory = b.path("src/protobuf"),
        .source_files = &.{
            "src/protobuf/agent_payload.proto",
        },
        .include_directories = &.{},
    });

    gen_proto.dependOn(&protoc_step.step);
}
