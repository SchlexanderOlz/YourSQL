const std = @import("std");

pub const SelectManager = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) SelectManager {
        return SelectManager{allocator};
    }
    pub fn where() !void {}
};
