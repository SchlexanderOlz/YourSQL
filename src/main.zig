const std = @import("std");
const DataManager = @import("./base/base.zig").DataManager;

pub fn main() !void {
    const allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    _ = allocator;
    var file: std.fs.File = try std.fs.cwd().createFile("test.zb", .{ .read = true });
    defer file.close();
    var dataManager = DataManager.init(std.heap.page_allocator, &file);

    const name = "myTable";
    const fields = .{ "myField", "myColum" };
    const types = .{ 1, 3 };
    try dataManager.createTable(name, &fields, &types);
}
