const std = @import("std");
const SelectManger = @import("select_manager.zig").SelectManager;
const String = @import("../string/string.zig").String;
const ArrayList = std.ArrayList;
const TableIterator = @import("./table_iterator.zig").TableIterator;

const SearchError = error{ NotFound, MetaEndNotFound };
const MetaEndNotFoundError = error{MetaEndNotFound};
const DataBaseError = error{ToLongId};

const LengthOffset: comptime_int = 1;
const StringOffset: comptime_int = 2;

pub const DataManager = struct {
    allocator: std.mem.Allocator,
    connection: std.fs.File,

    pub fn init(allocator: std.mem.Allocator, connection: std.fs.File) DataManager {
        return DataManager{ .allocator = allocator, .connection = connection };
    }

    pub fn deinit(self: DataManager) void {
        self.connection.close();
        return;
    }

    pub fn select(self: *DataManager, table: []const u8, columns: []const []const u8, equals: []const []const u8) !SelectManger {
        _ = equals;
        // myColumn, yourColumn FROM tableName WHERE myColumn = "10" AND yourColumn = 5

        const columnIndeces = try self.getIndexesOfTableColumns(table, columns);
        _ = columnIndeces;

        try self.moveCursorToEndOfMeta();
        var buff: [1024]u8 = undefined;
        @memset(&buff, 0);
        const bytesRead = try self.connection.read(&buff);
        std.debug.print("{s}", .{buff[0..bytesRead]});

        unreachable();
        return SelectManger.init(self.allocator);
    }

    pub fn moveCursorToEndOfMeta(self: *DataManager) !void {
        try self.connection.seekTo(0);

        var buff: []u8 = try self.readCurrentToBuff(1024);
        var bytesRead = buff.len;
        if (bytesRead == 0) return;
        var globalSize: usize = 0;
        while (bytesRead != 0) {
            var i: usize = 0;
            while (i < bytesRead) {
                // In case of the last table the 0 byte would be at position 1
                if (buff[i + 1] == 0) {
                    try self.connection.seekTo(i + globalSize);
                    return;
                }
                const nextSize = buff[i];

                i += nextSize;
            }
            // If gone through the entire buffer the next 1024 bytes are read
            buff = try self.readCurrentToBuff(1024);
            bytesRead = buff.len;
            globalSize += i;
        }
        try self.connection.seekTo(0);
        return SearchError.MetaEndNotFound;
    }

    /// Reads the next *size* bytes from the file and returns them.
    fn readCurrentToBuff(self: *const DataManager, comptime size: usize) ![]u8 {
        var buff: [size]u8 = undefined;
        @memset(&buff, 0);
        const bytesRead = try self.connection.read(&buff);
        return buff[0..bytesRead];
    }

    /// Reads the next *size* bytes from the file and returns them. The cursor stays
    /// at the same positon.
    fn readCurrentStaticToBuff(self: *const DataManager, comptime size: usize) ![]u8 {
        const oldPos = self.connection.getPos();
        const data = self.readCurrentToBuff(size);
        self.connection.seekTo(oldPos);
        return data;
    }

    /// Creates a new table in the database.
    /// Moves the cursor to position 0
    pub fn createTable(self: *DataManager, name: []const u8, fields: []const []const u8, typeIds: []const u8) !void {
        defer self.connection.seekTo(0) catch |err| {
            errdefer err;
        };

        if (name.len > 255) {
            return DataBaseError.ToLongId;
        }
        var buff: []u8 = try self.readCurrentToBuff(512);

        if (buff.len == 0) {
            const tableInfo = try self.createTableMeta(name, fields, typeIds);
            _ = try self.connection.write(tableInfo);
            return;
        }

        var i: usize = 0;
        while (true) : (i += 1) {
            if (buff[i] != 0) {
                continue;
            }
            const tableInfo = try self.createTableMeta(name, fields, typeIds);
            try self.connection.seekTo(i - 1);
            _ = try self.connection.write(tableInfo);
            break;
        }
    }

    fn createTableMeta(self: *const DataManager, name: []const u8, fields: []const []const u8, typeIds: []const u8) ![]u8 {
        var tableInfo = ArrayList(u8).init(self.allocator);
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

    pub fn getCursorPosition(self: *const DataManager) !usize {
        return try self.connection.getPos();
    }

    // Moves the cursor to the beginning, including the complete length, on pos 0,
    // of the requested table
    fn moveCursorToTable(self: *DataManager, tableName: []const u8) !void {
        var buff: []u8 = try self.readCurrentToBuff(1024);

        var i: usize = 0;
        while (i < buff.len) {
            if (buff[i + LengthOffset] != tableName.len) {
                i += buff[i];
            }

            if (std.mem.eql(u8, buff[i + StringOffset .. i + StringOffset + tableName.len], tableName)) {
                try self.connection.seekTo(i);
                return;
            }
            i += buff[i];
        }
        return SearchError.NotFound;
    }

    // TODO: This currently only is applicable for u8 sizes and not usize
    // Return the value of the current cursor-position
    pub fn getCurrentPosValue(self: *DataManager) !u8 {
        return try self.readCurrentStaticToBuff(1)[0];
    }

    // Moves the cursor further to the requested column. Assumes that
    // the cursor is at the position of the table
    fn moveCursorToColumn(self: *DataManager, columnName: []const u8) !void {
        const tableSize = try self.getCurrentPosValue();
        const oldPos = try self.connection.getPos();

        var metaTable: []u8 = try self.allocator.alloc(u8, tableSize);
        defer self.allocator.free(metaTable);
        @memset(metaTable, 0);
        const bytesRead = try self.connection.read(metaTable);
        try self.connection.seekTo(oldPos);

        var i: usize = @intCast(metaTable[0] + 1);
        while (i < bytesRead) {
            if (metaTable[i + LengthOffset] != columnName.len) {
                i += metaTable[i + LengthOffset] + 2;
                continue;
            }

            if (std.mem.eql(u8, metaTable[i + StringOffset .. i + StringOffset + columnName.len], columnName)) {
                try self.connection.seekBy(@intCast(i));
                return;
            }
        }
        return SearchError.NotFound;
    }

    // Returns the index in the file from the columns, in the same order as the requested columns
    pub fn getIndexesOfTableColumns(self: *DataManager, tableName: []const u8, columns: []const []const u8) ![]usize {
        const oldPos = try self.connection.getPos();
        defer self.connection.seekTo(oldPos) catch |err| {
            errdefer err;
        };

        try self.moveCursorToTable(tableName);
        var buff: [1024]u8 = undefined;
        @memset(&buff, 0);
        const bytesRead = try self.connection.read(&buff);
        var tableIterator = TableIterator.from_slice(buff[0..bytesRead]);

        var indexes = try ArrayList(usize).initCapacity(self.allocator, columns.len);
        for (columns) |column| {
            var i: usize = 0;
            while (tableIterator.next()) |metaColumn| {
                defer i += 1;
                if (metaColumn[LengthOffset] != column.len) {
                    continue;
                }
                if (std.mem.eql(u8, metaColumn[StringOffset..], column)) {
                    try indexes.append(i);
                    break;
                }
            }
            tableIterator.reset();
        }
        return try indexes.toOwnedSlice();
    }
};

// ---Tests-Start---

test "MoveToCreatedTest" {
    const name = "myTable";
    const fields = .{ "myField", "myColumn" };
    const types = .{ 1, 3 };

    const otherName = "otherTable";
    const otherFields = .{ "otherMyField", "otherMyColumn" };
    const otherTypes = .{ 60, 5 };

    var file: std.fs.File = try std.fs.cwd().createFile("test.zb", .{ .read = true });
    var dataManager = DataManager.init(std.heap.page_allocator, &file);
    defer dataManager.deinit();

    try dataManager.createTable(name, &fields, &types);
    try dataManager.createTable(otherName, &otherFields, &otherTypes);

    try dataManager.moveCursorToTable(otherName);
    var buff: [512]u8 = undefined;
    @memset(&buff, 0);

    _ = try dataManager.connection.read(&buff);
    try std.testing.expect(buff[LengthOffset] == otherName.len);
    try std.testing.expect(std.mem.eql(u8, buff[StringOffset .. StringOffset + otherName.len], otherName));
}
