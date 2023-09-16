const std = @import("std");
const SelectManger = @import("select_manager.zig").SelectManager;
const String = @import("../string/string.zig").String;
const ArrayList = std.ArrayList;

pub const DataManager = struct {
    allocator: std.mem.Allocator,
    connection: *std.fs.File,

    pub fn init(allocator: std.mem.Allocator, connection: *std.fs.File) DataManager {
        return DataManager{ allocator, connection };
    }

    pub fn select(this: *DataManager, fields: [][]u8, table: []u8, equals: [][]u8) SelectManger {
        _ = equals;
        _ = table;
        _ = fields;

        return SelectManger.init(this.allocator);
    }

    pub fn createTable(this: *DataManager, name: []u8, fields: [][]u8, typeIds: []u8) !void {
        var buff: [512]u8 = undefined;
        @memset(buff, 0);
        const bytesRead = this.connection.read(buff);
        var i = 0;
        while (i < bytesRead) : (i += 1) {
            if (bytesRead[i] != 0) {
                continue;
            }
            var tableInfo: ArrayList(u8) = ArrayList(u8).init(this.allocator);
            tableInfo.append(name.len);
            tableInfo.appendSlice(name);

            var j = 0;
            while (j < fields.len) : (j += 1) {
                tableInfo.append(fields[j].len);
                tableInfo.appendSlice(fields[j]);
                tableInfo.append(typeIds[j]);
            }

            this.connection.seekTo(i - 1);
            this.connection.write(try tableInfo.toOwnedSlice());
        }
    }

    fn getMetaColumnIndex(this: *DataManager, columnName: *[]u8) !u8 {
        _ = columnName;
        var buff: [512]u8 = undefined;
        @memset(buff, 0);
        const bytesRead = try this.connection.read(buff);
        const header = buff[0..bytesRead];
        _ = header;
    }
};
