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
        .root_source_file = b.path("main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const client_mod = b.createModule(.{
        .root_source_file = b.path("src/client.zig"),
        .target = target,
        .optimize = optimize,
    });

    const client_example_mod = b.createModule(.{
        .root_source_file = b.path("client_example.zig"),
        .target = target,
        .optimize = optimize,
    });

    // End-to-end test programs: the real server on one side, the real client on
    // the other. They build with `zig build`, so the gate compiles them too.
    const e2e_server_mod = b.createModule(.{
        .root_source_file = b.path("tests/e2e_server.zig"),
        .target = target,
        .optimize = optimize,
    });

    const e2e_concurrency_mod = b.createModule(.{
        .root_source_file = b.path("tests/e2e_concurrency.zig"),
        .target = target,
        .optimize = optimize,
    });

    const e2e_pipelined_mod = b.createModule(.{
        .root_source_file = b.path("tests/e2e_pipelined.zig"),
        .target = target,
        .optimize = optimize,
    });

    const e2e_client_mod = b.createModule(.{
        .root_source_file = b.path("tests/e2e_client.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Modules can depend on one another using the `std.Build.Module.addImport` function.
    // This is what allows Zig source code to use `@import("foo")` where 'foo' is not a
    // file path. In this case, we set up server_mod to import lib_mod.
    server_mod.addImport("wildb_lib", lib_mod);
    client_mod.addImport("wildb_lib", lib_mod);
    client_example_mod.addImport("wildb_lib", lib_mod);
    e2e_server_mod.addImport("wildb_lib", lib_mod);
    e2e_concurrency_mod.addImport("wildb_lib", lib_mod);
    e2e_client_mod.addImport("wildb_lib", lib_mod);
    e2e_pipelined_mod.addImport("wildb_lib", lib_mod);

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

    // Create the client example executable
    const client_example_exe = b.addExecutable(.{
        .name = "wild-client-example",
        .root_module = client_example_mod,
    });

    // Install all executables
    b.installArtifact(server_exe);
    b.installArtifact(client_exe);
    b.installArtifact(client_example_exe);

    const e2e_server_exe = b.addExecutable(.{
        .name = "e2e-server",
        .root_module = e2e_server_mod,
    });

    const e2e_client_exe = b.addExecutable(.{
        .name = "e2e-client",
        .root_module = e2e_client_mod,
    });

    const e2e_concurrency_exe = b.addExecutable(.{
        .name = "e2e-concurrency",
        .root_module = e2e_concurrency_mod,
    });

    const e2e_pipelined_exe = b.addExecutable(.{
        .name = "e2e-pipelined",
        .root_module = e2e_pipelined_mod,
    });

    b.installArtifact(e2e_server_exe);
    b.installArtifact(e2e_client_exe);
    b.installArtifact(e2e_concurrency_exe);
    b.installArtifact(e2e_pipelined_exe);

    // The throughput benchmark. It builds with `zig build`, so it cannot rot
    // into something that no longer compiles.
    const benchmark_mod = b.createModule(.{
        .root_source_file = b.path("benchmark.zig"),
        .target = target,
        .optimize = optimize,
    });

    const benchmark_exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = benchmark_mod,
    });

    b.installArtifact(benchmark_exe);

    // Named steps, so `zig build benchmark` and `zig build run-benchmark --
    // --compare` work. A gate step that only exists inside `install` is hard
    // to discover.
    const benchmark_step = b.step("benchmark", "Build the throughput benchmark");
    benchmark_step.dependOn(&benchmark_exe.step);

    const run_benchmark_cmd = b.addRunArtifact(benchmark_exe);
    run_benchmark_cmd.step.dependOn(b.getInstallStep());
    run_benchmark_cmd.addPassthruArgs();

    const run_benchmark_step = b.step("run-benchmark", "Run the throughput benchmark");
    run_benchmark_step.dependOn(&run_benchmark_cmd.step);

    // Create run steps for all executables
    const run_server_cmd = b.addRunArtifact(server_exe);
    const run_client_cmd = b.addRunArtifact(client_exe);
    const run_client_example_cmd = b.addRunArtifact(client_example_exe);

    run_server_cmd.step.dependOn(b.getInstallStep());
    run_client_cmd.step.dependOn(b.getInstallStep());
    run_client_example_cmd.step.dependOn(b.getInstallStep());

    // Allow passing arguments to all executables
    run_server_cmd.addPassthruArgs();
    run_client_cmd.addPassthruArgs();
    run_client_example_cmd.addPassthruArgs();

    // Create build steps for running each executable
    const run_server_step = b.step("run-server", "Run the WILD server");
    run_server_step.dependOn(&run_server_cmd.step);

    const run_client_step = b.step("run-client", "Run the WILD client");
    run_client_step.dependOn(&run_client_cmd.step);

    const run_client_example_step = b.step("run-client-example", "Run the WILD client example");
    run_client_example_step.dependOn(&run_client_example_cmd.step);

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

    // The slab allocator tests live in src/test_slab_allocator.zig. Nothing
    // imports that file, so a test step of its own is the only way they run.
    const slab_test_mod = b.createModule(.{
        .root_source_file = b.path("src/test_slab_allocator.zig"),
        .target = target,
        .optimize = optimize,
    });

    const slab_unit_tests = b.addTest(.{
        .root_module = slab_test_mod,
    });

    const run_slab_unit_tests = b.addRunArtifact(slab_unit_tests);

    // Database round trips through the same static allocator the server uses.
    const storage_test_mod = b.createModule(.{
        .root_source_file = b.path("tests/test_wild_storage.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_test_mod.addImport("wildb_lib", lib_mod);

    const storage_unit_tests = b.addTest(.{
        .root_module = storage_test_mod,
    });

    const run_storage_unit_tests = b.addRunArtifact(storage_unit_tests);

    // Batch reads must return exactly what single reads return, however the
    // prefetching reorders the memory traffic.
    const batch_read_test_mod = b.createModule(.{
        .root_source_file = b.path("tests/test_batch_read.zig"),
        .target = target,
        .optimize = optimize,
    });
    batch_read_test_mod.addImport("wildb_lib", lib_mod);

    const batch_read_unit_tests = b.addTest(.{
        .root_module = batch_read_test_mod,
    });

    const run_batch_read_unit_tests = b.addRunArtifact(batch_read_unit_tests);

    // A delete has to survive a crash. These pin the WAL entry that carries a
    // delete, and the readers that must honour it.
    const wal_delete_test_mod = b.createModule(.{
        .root_source_file = b.path("tests/test_wal_delete.zig"),
        .target = target,
        .optimize = optimize,
    });
    wal_delete_test_mod.addImport("wildb_lib", lib_mod);

    const wal_delete_unit_tests = b.addTest(.{
        .root_module = wal_delete_test_mod,
    });

    const run_wal_delete_unit_tests = b.addRunArtifact(wal_delete_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_server_unit_tests.step);
    test_step.dependOn(&run_client_unit_tests.step);
    test_step.dependOn(&run_slab_unit_tests.step);
    test_step.dependOn(&run_storage_unit_tests.step);
    test_step.dependOn(&run_batch_read_unit_tests.step);
    test_step.dependOn(&run_wal_delete_unit_tests.step);

    // Zig analyses code lazily: a function nothing calls is never type-checked.
    // deep_check_main.zig references every public declaration, so the compiler
    // must analyse each one. This step catches code that `zig build` skips.
    const deep_check_mod = b.createModule(.{
        .root_source_file = b.path("deep_check_main.zig"),
        .target = target,
        .optimize = optimize,
    });
    deep_check_mod.addImport("wildb_lib", lib_mod);

    const deep_check_exe = b.addExecutable(.{
        .name = "deep-check",
        .root_module = deep_check_mod,
    });

    const deep_check_step = b.step(
        "deep-check",
        "Type-check every public declaration, including code nothing calls",
    );
    deep_check_step.dependOn(&deep_check_exe.step);

    // The files under tests/ are standalone programs. Nothing else builds them,
    // so a broken import used to go unnoticed.
    const cache_detection_mod = b.createModule(.{
        .root_source_file = b.path("tests/test_cache_detection.zig"),
        .target = target,
        .optimize = optimize,
    });
    cache_detection_mod.addImport("wildb_lib", lib_mod);

    const cache_detection_exe = b.addExecutable(.{
        .name = "test-cache-detection",
        .root_module = cache_detection_mod,
    });

    const check_tests_step = b.step("check-tests", "Compile the standalone test programs");
    check_tests_step.dependOn(&cache_detection_exe.step);
}
