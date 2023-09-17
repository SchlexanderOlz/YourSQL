const std = @import("std");
const SelectManger = @import("select_manager.zig").SelectManager;
const String = @import("../string/string.zig").String;
const ArrayList = std.ArrayList;

const SearchError = error{NotFound};
const DataBaseError = error{ToLongId};

const LengthOffset: comptime_int = 1;
const StringOffset: comptime_int = 2;

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

    pub fn select(this: *const DataManager, columns: [][]u8, table: []u8, equals: [][]u8) SelectManger {
        // myColumn, yourColumn FROM tableName WHERE myColumn = "10" AND yourColumn = 5
        _ = equals;
        _ = table;

        for (columns) |element| {
            const idx = this.getMetaColumnIndex(element);
            _ = idx;
        }

        return SelectManger.init(this.allocator);
    }

    pub fn createTable(this: *DataManager, name: []const u8, fields: []const []const u8, typeIds: []const u8) !void {
        defer this.connection.seekTo(0) catch |err| {
            errdefer err;
        };

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
            break;
        }
    }

    fn createTableMeta(this: *const DataManager, name: []const u8, fields: []const []const u8, typeIds: []const u8) ![]u8 {
        var tableInfo = ArrayList(u8).init(this.allocator);
        defer tableInfo.deinit();

        const totalSizeIdx = 0;

        try tableInfo.append(0);
        try tableInfo.append(@intCast(name.len));
        try tableInfo.appendSlice(name);

        var j: usize = 0;
        while (j < fields.len) : (j += 1) {
            try tableInfo.append(typeIds[j]);
            try tableInfo.append(@intCast(fields[j].len));
            try tableInfo.appendSlice(fields[j]);
        }
        try tableInfo.append(0x0);
        const slice = try tableInfo.toOwnedSlice();
        slice[totalSizeIdx] = @intCast(slice.len - 2);
        return slice;
    }

    // Moves the cursor to the beginning, including the complete length, on pos 0,
    // of the requested table
    pub fn moveCursorToTable(this: *DataManager, tableName: []const u8) !void {
        var buff: [1024]u8 = undefined;
        @memset(&buff, 0);

        var bytesRead: usize = try this.connection.read(&buff);

        var i: usize = 0;
        while (i < bytesRead) {
            if (buff[i + LengthOffset] != tableName.len) {
                i += buff[i];
            }

            if (std.mem.eql(u8, buff[i + StringOffset .. i + StringOffset + tableName.len], tableName)) {
                try this.connection.seekTo(i);
                return;
            }
            i += buff[i];
        }
        return SearchError.NotFound;
    }

    // Moves the cursor further to the requested column. Assumes that
    // the cursor is at the position of the table
    pub fn moveCursorToColumn(this: *DataManager, columnName: []const u8) !void {
        var buff: [1]u8 = undefined;
        @memset(&buff, 0);
        _ = try this.connection.read(&buff);
        const oldPos = try this.connection.getPos();

        var metaTable: []u8 = try this.allocator.alloc(u8, buff[0]);
        @memset(metaTable, 0);
        const bytesRead = try this.connection.read(metaTable);
        std.debug.assert(bytesRead == buff[0]);
        try this.connection.seekTo(oldPos);

        var i: usize = @intCast(metaTable[0] + 1);
        while (i < bytesRead) {
            if (metaTable[i + LengthOffset] != columnName.len) {
                i += metaTable[i + LengthOffset] + 2;
                continue;
            }

            if (std.mem.eql(u8, metaTable[i + StringOffset .. i + StringOffset + columnName.len], columnName)) {
                try this.connection.seekBy(@intCast(i));
                return;
            }
        }
        return SearchError.NotFound;
    }
};
