const std = @import("std");
pub const TableIterator = struct {
    data: []u8,
    index: usize,

    pub fn from_slice(dataTable: []u8) TableIterator {
        return TableIterator{ .data = dataTable, .index = dataTable[1] + 2 };
    }

    pub fn next(self: *TableIterator) ?[]u8 {
        const currentLen = self.data[self.index + 1];
        const nextBegin = self.index + currentLen + 2;
        if (nextBegin + 1 >= self.data.len) {
            return null;
        }

        const newData = self.data[self.index..nextBegin];
        self.index = nextBegin;
        return newData;
    }

    pub fn reset(self: *TableIterator) void {
        self.index = self.data[1] + 2;
    }
};
