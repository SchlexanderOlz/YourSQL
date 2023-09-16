const std = @import("std");
const mem = std.mem;

pub const String = struct {
    data: []u8,
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator) !String {
        return String{ .data = undefined, .allocator = allocator };
    }

    pub fn from_slice(allocator: mem.Allocator, data: []const u8) !String {
        var string = try init(allocator);
        string.data = try allocator.alloc(u8, data.len * @sizeOf(u8));
        @memcpy(string.data, data);
        return string;
    }

    pub fn as_str(self: *const String) []u8 {
        return self.data;
    }

    pub fn size(self: *const String) usize {
        return self.data.len;
    }

    pub fn split(self: *const String, delimiter: u8) ![]String {
        var result: []String = try self.allocator.alloc(comptime String, 0);

        var start: usize = 0;
        var end: usize = 0;
        while (end <= self.size()) : (end += 1) {
            if (self.data[start] == delimiter) {
                start = end;
                continue;
            }

            if (self.data[end] == delimiter) {
                result = try self.allocator.realloc(result, result.len + 1);
                result[result.len - 1] = try String.from_slice(self.allocator, self.data[start..end]);
                end += 1;
                start = end;
            }
        }
        return result;
    }
};
