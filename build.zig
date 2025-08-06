const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // This creates a "module", which represents a collection of source files alongside
    // some compilation options, such as optimization mode and linked system libraries.
    // Every executable or library we compile will be based on one or more modules.
    const lib_mod = b.createModule(.{
        // `root_source_file` is the Zig "entry point" of the module. If a module
        // only contains e.g. external object files, you can make this `null`.
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // We will also create modules for our entry points
    const server_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const client_mod = b.createModule(.{
        .root_source_file = b.path("src/client.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Modules can depend on one another using the `std.Build.Module.addImport` function.
    // This is what allows Zig source code to use `@import("foo")` where 'foo' is not a
    // file path. In this case, we set up server_mod to import lib_mod.
    server_mod.addImport("wildb_lib", lib_mod);

    // Now, we will create a static library based on the module we created above.
    // This creates a `std.Build.Step.Compile`, which is the build step responsible
    // for actually invoking the compiler.
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "wildb",
        .root_module = lib_mod,
    });

    // This declares intent for the library to be installed into the standard
    // location when the user invokes the "install" step (the default step when
    // running `zig build`).
    b.installArtifact(lib);

    // Create the server/CLI executable
    const server_exe = b.addExecutable(.{
        .name = "wild",
        .root_module = server_mod,
    });

    // Create the client executable
    const client_exe = b.addExecutable(.{
        .name = "wild-client",
        .root_module = client_mod,
    });

    // Install both executables
    b.installArtifact(server_exe);
    b.installArtifact(client_exe);

    // Create run steps for both executables
    const run_server_cmd = b.addRunArtifact(server_exe);
    const run_client_cmd = b.addRunArtifact(client_exe);

    run_server_cmd.step.dependOn(b.getInstallStep());
    run_client_cmd.step.dependOn(b.getInstallStep());

    // Allow passing arguments to both executables
    if (b.args) |args| {
        run_server_cmd.addArgs(args);
        run_client_cmd.addArgs(args);
    }

    // Create build steps for running each executable
    const run_server_step = b.step("run-server", "Run the WILD server");
    run_server_step.dependOn(&run_server_cmd.step);

    const run_client_step = b.step("run-client", "Run the WILD client");
    run_client_step.dependOn(&run_client_cmd.step);

    // Default run step runs the server for backward compatibility
    const run_step = b.step("run", "Run the WILD server");
    run_step.dependOn(&run_server_cmd.step);

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const server_unit_tests = b.addTest(.{
        .root_module = server_mod,
    });

    const client_unit_tests = b.addTest(.{
        .root_module = client_mod,
    });

    const run_server_unit_tests = b.addRunArtifact(server_unit_tests);
    const run_client_unit_tests = b.addRunArtifact(client_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_server_unit_tests.step);
    test_step.dependOn(&run_client_unit_tests.step);
}
