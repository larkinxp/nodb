const std = @import("std");
const insertOneResult = @import("./results.zig").insertOneResult;

pub fn Hooks(comptime K: type, comptime V: type) type {
    return struct {
        pub fn init() Hooks(K, V) {
            return Hooks(K, V){};
        }
        pub fn onInsert(key: K, value: V, result: insertOneResult) !void {
            _ = key;
            _ = value;
            _ = result;
        }
        pub fn onUpdate(key: K, value: V, result: insertOneResult) !void {
            _ = key;
            _ = value;
            _ = result;
        }
        pub fn onRemove(key: K, value: V, result: insertOneResult) !void {
            _ = key;
            _ = value;
            _ = result;
        }
    };
}
