const std = @import("std");

pub const SlabAllocator = struct {
    const Self = @This();

    // Size classes - powers of 2 from 8 bytes to 64KB
    const MIN_SIZE_CLASS = 8;
    const MAX_SIZE_CLASS = 65536;
    const SIZE_CLASS_COUNT = 14; // log2(65536/8) + 1

    // Slab configuration
    const SLAB_SIZE = 2 * 1024 * 1024; // 2MB slabs for good cache locality
    const MAX_SLABS_PER_CLASS = 16; // Limit memory usage per size class

    const FreeBlock = struct {
        next: ?*FreeBlock,
    };

    const Slab = struct {
        memory: []u8,
        free_list: ?*FreeBlock,
        object_size: usize,
        objects_per_slab: usize,
        free_count: usize,
        next_slab: ?*Slab,
    };

    const SizeClass = struct {
        object_size: usize,
        slabs: ?*Slab,
        slab_count: usize,
    };

    backing_allocator: std.mem.Allocator,
    size_classes: [SIZE_CLASS_COUNT]SizeClass,
    allocated_slabs: std.ArrayList(*Slab),

    pub fn init(backing_allocator: std.mem.Allocator) Self {
        var size_classes: [SIZE_CLASS_COUNT]SizeClass = undefined;
        
        // Initialize size classes with powers of 2
        var size: usize = MIN_SIZE_CLASS;
        for (0..SIZE_CLASS_COUNT) |i| {
            size_classes[i] = SizeClass{
                .object_size = size,
                .slabs = null,
                .slab_count = 0,
            };
            size *= 2;
        }

        return Self{
            .backing_allocator = backing_allocator,
            .size_classes = size_classes,
            .allocated_slabs = std.ArrayList(*Slab).init(backing_allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all slabs
        for (self.allocated_slabs.items) |slab| {
            self.backing_allocator.free(slab.memory);
            self.backing_allocator.destroy(slab);
        }
        self.allocated_slabs.deinit();
    }

    pub fn allocator(self: *Self) std.mem.Allocator {
        return std.mem.Allocator{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .free = free,
                .remap = remap,
            },
        };
    }

    fn getSizeClassIndex(size: usize) usize {
        std.debug.assert(size >= MIN_SIZE_CLASS and size <= MAX_SIZE_CLASS);
        
        // Find the smallest power of 2 that can hold the size
        var class_size: usize = MIN_SIZE_CLASS;
        var index: usize = 0;
        
        while (class_size < size and index < SIZE_CLASS_COUNT - 1) {
            class_size *= 2;
            index += 1;
        }
        
        return index;
    }

    fn roundUpToSizeClass(size: usize) usize {
        if (size <= MIN_SIZE_CLASS) return MIN_SIZE_CLASS;
        if (size > MAX_SIZE_CLASS) return size; // Fall back to backing allocator
        
        // Round up to next power of 2
        var class_size: usize = MIN_SIZE_CLASS;
        while (class_size < size) {
            class_size = class_size * 2;
        }
        
        return class_size;
    }

    fn createSlab(self: *Self, size_class: *SizeClass) !*Slab {
        // Don't create too many slabs per size class
        if (size_class.slab_count >= MAX_SLABS_PER_CLASS) {
            return error.TooManySlabs;
        }

        const slab = try self.backing_allocator.create(Slab);
        const memory = try self.backing_allocator.alignedAlloc(u8, std.heap.page_size_min, SLAB_SIZE);
        
        const objects_per_slab = SLAB_SIZE / size_class.object_size;
        
        slab.* = Slab{
            .memory = memory,
            .free_list = null,
            .object_size = size_class.object_size,
            .objects_per_slab = objects_per_slab,
            .free_count = objects_per_slab,
            .next_slab = size_class.slabs,
        };

        // Initialize free list
        var current_offset: usize = 0;
        var prev_block: ?*FreeBlock = null;
        
        for (0..objects_per_slab) |_| {
            const block: *FreeBlock = @ptrCast(@alignCast(&memory[current_offset]));
            block.next = prev_block;
            prev_block = block;
            current_offset += size_class.object_size;
        }
        
        slab.free_list = prev_block;
        
        // Link slab into size class
        size_class.slabs = slab;
        size_class.slab_count += 1;
        
        // Track slab for cleanup
        try self.allocated_slabs.append(slab);
        
        return slab;
    }

    fn allocFromSlab(slab: *Slab) ?*anyopaque {
        if (slab.free_list == null) return null;
        
        const block = slab.free_list.?;
        slab.free_list = block.next;
        slab.free_count -= 1;
        
        return @ptrCast(block);
    }

    fn freeToSlab(slab: *Slab, ptr: *anyopaque) void {
        const block: *FreeBlock = @ptrCast(@alignCast(ptr));
        block.next = slab.free_list;
        slab.free_list = block;
        slab.free_count += 1;
    }

    fn findSlabForPointer(self: *Self, ptr: *anyopaque) ?*Slab {
        const addr = @intFromPtr(ptr);
        
        for (self.allocated_slabs.items) |slab| {
            const slab_start = @intFromPtr(slab.memory.ptr);
            const slab_end = slab_start + slab.memory.len;
            
            if (addr >= slab_start and addr < slab_end) {
                return slab;
            }
        }
        
        return null;
    }

    fn alloc(ctx: *anyopaque, len: usize, ptr_align: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // For large allocations or unusual alignments, use backing allocator
        const align_bytes = ptr_align.toByteUnits();
        if (len > MAX_SIZE_CLASS or align_bytes > std.heap.page_size_min) {
            return self.backing_allocator.rawAlloc(len, ptr_align, ret_addr);
        }
        
        const size_class_size = roundUpToSizeClass(len);
        const size_class_index = getSizeClassIndex(size_class_size);
        const size_class = &self.size_classes[size_class_index];
        
        // Try to allocate from existing slabs
        var current_slab = size_class.slabs;
        while (current_slab) |slab| {
            if (allocFromSlab(slab)) |ptr| {
                return @ptrCast(ptr);
            }
            current_slab = slab.next_slab;
        }
        
        // Create new slab if needed
        const new_slab = self.createSlab(size_class) catch {
            return self.backing_allocator.rawAlloc(len, ptr_align, ret_addr);
        };
        
        // Allocate from the new slab
        const ptr = allocFromSlab(new_slab) orelse unreachable;
        return @ptrCast(ptr);
    }

    fn resize(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // Find which slab this belongs to
        const slab = self.findSlabForPointer(buf.ptr) orelse {
            // Not our allocation, try backing allocator
            return self.backing_allocator.rawResize(buf, buf_align, new_len, ret_addr);
        };
        
        // Slab allocations are fixed size - can only shrink within the same size class
        const current_size_class = roundUpToSizeClass(buf.len);
        const new_size_class = roundUpToSizeClass(new_len);
        
        return current_size_class == new_size_class and new_len <= slab.object_size;
    }

    fn free(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, ret_addr: usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // Find which slab this belongs to
        const slab = self.findSlabForPointer(buf.ptr) orelse {
            // Not our allocation, use backing allocator
            self.backing_allocator.rawFree(buf, buf_align, ret_addr);
            return;
        };
        
        freeToSlab(slab, buf.ptr);
    }

    fn remap(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // Find which slab this belongs to
        const slab = self.findSlabForPointer(buf.ptr) orelse {
            // Not our allocation, try backing allocator
            return self.backing_allocator.rawRemap(buf, buf_align, new_len, ret_addr);
        };
        
        // Slab allocations can't be remapped - size is fixed
        const current_size_class = roundUpToSizeClass(buf.len);
        const new_size_class = roundUpToSizeClass(new_len);
        
        if (current_size_class == new_size_class and new_len <= slab.object_size) {
            return buf.ptr;
        }
        
        return null;
    }

    const SizeClassStats = struct {
        object_size: usize,
        slab_count: usize,
        free_objects: usize,
    };
    
    // Debug information
    pub fn getStats(self: *const Self) struct {
        total_slabs: usize,
        total_free_objects: usize,
        size_class_stats: [SIZE_CLASS_COUNT]SizeClassStats,
    } {
        var size_class_stats: [SIZE_CLASS_COUNT]SizeClassStats = undefined;
        
        var total_slabs: usize = 0;
        var total_free_objects: usize = 0;
        
        for (0..SIZE_CLASS_COUNT) |i| {
            const size_class = &self.size_classes[i];
            var free_objects: usize = 0;
            
            var current_slab = size_class.slabs;
            while (current_slab) |slab| {
                free_objects += slab.free_count;
                current_slab = slab.next_slab;
            }
            
            size_class_stats[i] = SizeClassStats{
                .object_size = size_class.object_size,
                .slab_count = size_class.slab_count,
                .free_objects = free_objects,
            };
            
            total_slabs += size_class.slab_count;
            total_free_objects += free_objects;
        }
        
        return .{
            .total_slabs = total_slabs,
            .total_free_objects = total_free_objects,
            .size_class_stats = size_class_stats,
        };
    }
};