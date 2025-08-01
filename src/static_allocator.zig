const std = @import("std");

pub const StaticAllocator = struct {
    const Self = @This();
    
    pub const State = enum {
        init,
        static,
        deinit,
    };
    
    state: State,
    backing_allocator: std.mem.Allocator,
    allocated_memory: std.ArrayList([]u8),
    
    pub fn init(backing_allocator: std.mem.Allocator) Self {
        return Self{
            .state = .init,
            .backing_allocator = backing_allocator,
            .allocated_memory = std.ArrayList([]u8).init(backing_allocator),
        };
    }
    
    pub fn transitionToStatic(self: *Self) void {
        std.debug.assert(self.state == .init);
        self.state = .static;
    }
    
    pub fn transitionToDeinit(self: *Self) void {
        std.debug.assert(self.state == .static);
        self.state = .deinit;
    }
    
    pub fn deinit(self: *Self) void {
        std.debug.assert(self.state == .deinit);
        
        // Free all allocated memory
        for (self.allocated_memory.items) |memory| {
            self.backing_allocator.free(memory);
        }
        self.allocated_memory.deinit();
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
    
    fn alloc(ctx: *anyopaque, len: usize, ptr_align: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // Only allow allocation during init state
        if (self.state != .init) {
            std.debug.panic("StaticAllocator: allocation attempted in {} state (only allowed in init state)", .{self.state});
        }
        
        const result = self.backing_allocator.rawAlloc(len, ptr_align, ret_addr) orelse return null;
        const memory = result[0..len];
        
        // Track allocated memory for cleanup
        self.allocated_memory.append(memory) catch {
            self.backing_allocator.rawFree(memory, ptr_align, ret_addr);
            return null;
        };
        
        return result;
    }
    
    fn resize(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        _ = ctx;
        _ = buf;
        _ = buf_align;
        _ = new_len;
        _ = ret_addr;
        
        // Never allow resize - all memory must be allocated upfront
        return false;
    }
    
    fn free(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, ret_addr: usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // Allow free during init (for temporary allocations) and deinit states
        // Static state should not free anything
        if (self.state == .static) {
            std.debug.panic("StaticAllocator: free attempted in static state (not allowed)", .{});
        }
        
        self.backing_allocator.rawFree(buf, buf_align, ret_addr);
    }
    
    fn remap(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        
        // Only allow remap during init state
        if (self.state != .init) {
            std.debug.panic("StaticAllocator: remap attempted in {} state (only allowed in init state)", .{self.state});
        }
        
        return self.backing_allocator.rawRemap(buf, buf_align, new_len, ret_addr);
    }
};