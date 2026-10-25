/// Reference every declaration of the given type so the compiler cannot skip a
/// function body. Zig analyses code lazily: an unreferenced function is never
/// type-checked, so `zig build` passes on code that cannot compile.
pub fn reach(comptime T: type) void {
    @setEvalBranchQuota(1_000_000);
    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => {
            for (info.@"struct".decl_names) |name| reachDecl(T, name);
        },
        .@"union" => {
            for (info.@"union".decl_names) |name| reachDecl(T, name);
        },
        .@"enum" => {
            for (info.@"enum".decl_names) |name| reachDecl(T, name);
        },
        else => {},
    }
}

/// Reference a module-level function. Taking its address forces the compiler to
/// analyse the body, which a plain type reference does not.
pub fn reachFn(comptime function: anytype) void {
    const pointer: *const @TypeOf(function) = &function;
    _ = pointer;
}

fn reachDecl(comptime T: type, comptime name: [:0]const u8) void {
    @setEvalBranchQuota(1_000_000);
    if (name.len > 0 and name[0] == '_') return;
    const value = @field(T, name);
    const Value = @TypeOf(value);
    const pointer: *const Value = &value;
    switch (@typeInfo(Value)) {
        .@"struct", .@"union", .@"enum" => reach(Value),
        else => _ = pointer,
    }
}
