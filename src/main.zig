const std = @import("std");
const DataManager = @import("./base/base.zig").DataManager;

pub fn main() !void {
    const name = "myTable";
    const fields = .{ "myField", "myColumn" };
    const types = .{ 1, 3 };

    const otherName = "otherTable";
    const otherFields = .{ "otherMyField", "otherMyColumn" };
    const otherTypes = .{ 60, 5 };

    var file: std.fs.File = try std.fs.cwd().createFile("test.zb", .{ .read = true });
    var dataManager = DataManager.init(std.heap.page_allocator, file);

    try dataManager.createTable(name, &fields, &types);
    try dataManager.createTable(otherName, &otherFields, &otherTypes);

    const indeces = try dataManager.getIndexesOfTableColumns(otherName, &otherFields);

    _ = try dataManager.select(name, &fields, &fields);

    std.debug.print("{any}", .{indeces});
}

pub fn executeThis(comptime function: (fn () void)) void {
    function();
}
