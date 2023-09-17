const std = @import("std");
const SelectManger = @import("select_manager.zig").SelectManager;
const String = @import("../string/string.zig").String;
const ArrayList = std.ArrayList;

const ColumnError = error{NotFound};
const DataBaseError = error{ToLongId};

pub const DataManager = struct {
    allocator: std.mem.Allocator,
    connection: *std.fs.File,

    pub fn init(allocator: std.mem.Allocator, connection: *std.fs.File) DataManager {
        return DataManager{ .allocator = allocator, .connection = connection };
    }

    pub fn deinit(this: *const DataManager) void {
        _ = this;
        return;
    }

    pub fn select(this: *const DataManager, fields: [][]u8, table: []u8, equals: [][]u8) SelectManger {
        _ = equals;
        _ = table;
        _ = fields;

        return SelectManger.init(this.allocator);
    }

    pub fn createTable(this: *DataManager, name: []const u8, fields: []const []const u8, typeIds: []const u8) !void {
        if (name.len > 255) {
            return DataBaseError.ToLongId;
        }
        var buff: [512]u8 = undefined;
        @memset(&buff, 0);
        const bytesRead = try this.connection.read(&buff);

        if (bytesRead == 0) {
            const tableInfo = try this.createTableMeta(name, fields, typeIds);
            _ = try this.connection.write(tableInfo);
            return;
        }

        var i: usize = 0;
        while (true) : (i += 1) {
            if (buff[i] != 0) {
                continue;
            }
            const tableInfo = try this.createTableMeta(name, fields, typeIds);
            try this.connection.seekTo(i - 1);
            _ = try this.connection.write(tableInfo);
        }
    }

    fn createTableMeta(this: *const DataManager, name: []const u8, fields: []const []const u8, typeIds: []const u8) ![]u8 {
        var tableInfo = ArrayList(u8).init(this.allocator);

        try tableInfo.append(@intCast(name.len));
        try tableInfo.appendSlice(name);

        var j: usize = 0;
        while (j < fields.len) : (j += 1) {
            try tableInfo.append(@intCast(fields[j].len));
            try tableInfo.appendSlice(fields[j]);
            try tableInfo.append(typeIds[j]);
        }
        const slice = try tableInfo.toOwnedSlice();
        return slice;
    }

    fn getMetaColumnIndex(this: *const DataManager, columnName: *[]u8) !usize {
        var buff: [512]u8 = undefined;
        comptime @memset(buff, 0);
        const bytesRead = try this.connection.read(buff);

        var i: usize = 0;
        while (i < bytesRead) {
            const data: u8 = @intCast(buff[i]);
            if (columnName.len != data) {
                i += data;
                continue;
            }
            const string = buff[i + 1 .. i + data];
            if (string == *columnName) {
                return i;
            }
            i += data;
        }
        return ColumnError.NotFound;
    }
};
