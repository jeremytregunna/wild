const std = @import("std");
const slab_allocator = @import("slab_allocator.zig");

test "slab allocator basic allocation and deallocation" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    // Test basic allocation
    const ptr1 = try allocator.alloc(u8, 16);
    defer allocator.free(ptr1);
    
    // Write to memory to ensure it's valid
    for (ptr1, 0..) |*byte, i| {
        byte.* = @intCast(i);
    }
    
    // Verify written data
    for (ptr1, 0..) |byte, i| {
        try std.testing.expect(byte == @as(u8, @intCast(i)));
    }
}

test "slab allocator different size classes" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    // Test various size classes
    const sizes = [_]usize{ 8, 16, 32, 64, 128, 256, 512, 1024 };
    var ptrs: [sizes.len][]u8 = undefined;
    
    // Allocate different sizes
    for (sizes, 0..) |size, i| {
        ptrs[i] = try allocator.alloc(u8, size);
        
        // Write pattern to verify memory is separate
        for (ptrs[i], 0..) |*byte, j| {
            byte.* = @intCast((i + j) % 256);
        }
    }
    
    // Verify patterns
    for (sizes, 0..) |_, i| {
        for (ptrs[i], 0..) |byte, j| {
            try std.testing.expect(byte == @as(u8, @intCast((i + j) % 256)));
        }
    }
    
    // Free all
    for (ptrs) |ptr| {
        allocator.free(ptr);
    }
}

test "slab allocator reuse freed memory" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    // Allocate and free many small objects
    var ptrs: [100][]u8 = undefined;
    
    // First round - allocate all
    for (&ptrs, 0..) |*ptr, i| {
        ptr.* = try allocator.alloc(u8, 32);
        ptr.*[0] = @intCast(i);
    }
    
    // Free every other one
    for (ptrs, 0..) |ptr, i| {
        if (i % 2 == 0) {
            allocator.free(ptr);
        }
    }
    
    // Allocate new ones - should reuse freed memory
    for (&ptrs, 0..) |*ptr, i| {
        if (i % 2 == 0) {
            ptr.* = try allocator.alloc(u8, 32);
            ptr.*[0] = @intCast(i + 100);
        }
    }
    
    // Verify data
    for (ptrs, 0..) |ptr, i| {
        if (i % 2 == 0) {
            try std.testing.expect(ptr[0] == @as(u8, @intCast(i + 100)));
        } else {
            try std.testing.expect(ptr[0] == @as(u8, @intCast(i)));
        }
    }
    
    // Free all remaining
    for (ptrs) |ptr| {
        allocator.free(ptr);
    }
}

test "slab allocator large allocation fallback" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    // Large allocation should fall back to backing allocator
    const large_ptr = try allocator.alloc(u8, 128 * 1024);
    defer allocator.free(large_ptr);
    
    // Should work normally
    large_ptr[0] = 42;
    large_ptr[large_ptr.len - 1] = 84;
    
    try std.testing.expect(large_ptr[0] == 42);
    try std.testing.expect(large_ptr[large_ptr.len - 1] == 84);
}

test "slab allocator stats" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    // Initial stats
    var stats = slab_alloc.getStats();
    try std.testing.expect(stats.total_slabs == 0);
    
    // Allocate some memory to create slabs
    const ptr1 = try allocator.alloc(u8, 16);
    const ptr2 = try allocator.alloc(u8, 64);
    const ptr3 = try allocator.alloc(u8, 256);
    
    stats = slab_alloc.getStats();
    try std.testing.expect(stats.total_slabs == 3); // Should create 3 slabs for different size classes
    
    allocator.free(ptr1);
    allocator.free(ptr2);
    allocator.free(ptr3);
}

test "slab allocator alignment" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    // Test that allocations are properly aligned
    const ptr8 = try allocator.alignedAlloc(u8, 8, 32);
    defer allocator.free(ptr8);
    try std.testing.expect(@intFromPtr(ptr8.ptr) % 8 == 0);
    
    const ptr16 = try allocator.alignedAlloc(u8, 16, 32);
    defer allocator.free(ptr16);
    try std.testing.expect(@intFromPtr(ptr16.ptr) % 16 == 0);
}

test "slab allocator stress test" {
    var slab_alloc = slab_allocator.SlabAllocator.init(std.testing.allocator);
    defer slab_alloc.deinit();
    
    const allocator = slab_alloc.allocator();
    
    var ptrs = std.ArrayList([]u8).init(std.testing.allocator);
    defer ptrs.deinit();
    
    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();
    
    // Random allocations and frees
    for (0..1000) |_| {
        if (ptrs.items.len > 0 and random.boolean()) {
            // Free a random pointer
            const index = random.uintLessThan(usize, ptrs.items.len);
            allocator.free(ptrs.items[index]);
            _ = ptrs.swapRemove(index);
        } else {
            // Allocate random size
            const sizes = [_]usize{ 8, 16, 32, 64, 128, 256, 512 };
            const size = sizes[random.uintLessThan(usize, sizes.len)];
            const ptr = try allocator.alloc(u8, size);
            try ptrs.append(ptr);
            
            // Write pattern
            for (ptr, 0..) |*byte, i| {
                byte.* = @intCast(i % 256);
            }
        }
    }
    
    // Free remaining
    for (ptrs.items) |ptr| {
        allocator.free(ptr);
    }
}