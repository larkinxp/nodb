const std = @import("std");

pub fn KeyValue(comptime K: type, comptime V: type) type {
    return struct {
        key: K,
        value: *V,
        current_position: usize,
        const Self: type = KeyValue(K, V);
        pub fn serialize(self: *Self, allocator: std.mem.Allocator) ![]const u8 {
            std.debug.print("SerializeValue.serialize\n", .{});
            const keyBytes = std.mem.toBytes(self.key);
            const valueBytes = std.mem.toBytes(self.value);
            const totalLength = keyBytes.len + valueBytes.len;
            var buffer = try allocator.alloc(u8, totalLength);
            std.mem.copyForwards(u8, buffer[0..keyBytes.len], &keyBytes);
            std.mem.copyForwards(u8, buffer[keyBytes.len..], &valueBytes);
            return buffer;
        }
        pub fn deserialize(bytes: []const u8, current_position: usize) Self {
            std.debug.print("SerializeValue.serialize\n", .{});
            const key: K = std.mem.bytesToValue(K, bytes[0..@sizeOf(K)]);
            var value: V = std.mem.bytesToValue(V, bytes[@sizeOf(K)..]);
            return Self{
                .key = key,
                .value = &value,
                .current_position = current_position,
            };
        }
        pub fn delete(self: *Self) void {
            self.key = 0;
        }
        pub fn deleted(self: *Self) bool {
            return self.key == 0;
        }
    };
}
