const std = @import("std");
const DataManager = @import("./base.zig").DataManager;

const allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
const name = "myTable";
const fields = .{ "myField", "myColumn" };
const types = .{ 1, 3 };

const otherName = "otherTable";
const otherFields = .{ "otherMyField", "otherMyColumn" };
const otherTypes = .{ 60, 5 };

test "CreateTableTest" {
    var file: std.fs.File = try std.fs.cwd().createFile("test.zb", .{ .read = true });
    var dataManager = DataManager.init(std.heap.page_allocator, &file);

    try dataManager.createTable(name, &fields, &types);
    var buff: [512]u8 = undefined;
    @memset(&buff, 0);
    _ = try file.read(&buff);
    try std.testing.expect(buff[1] == name.len);
    try std.testing.expect(std.mem.eql(u8, buff[2 .. buff[1] + 2], name));
    const fieldIdx = buff[1] + 3;
    try std.testing.expect(buff[fieldIdx] == fields[0].len);
    try std.testing.expect(std.mem.eql(u8, buff[fieldIdx + 1 .. fieldIdx + buff[fieldIdx] + 1], fields[0]));
}
