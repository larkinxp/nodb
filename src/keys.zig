const std = @import("std");

const Database = @import("./database.zig").Database;
const Error = @import("./error.zig").Error;

/// ?
pub const Strategy = enum {
    Random,
    Sequential,
};

/// ?
pub fn Keys(comptime K: type, comptime V: type) type {
    return struct {
        /// ?
        pub fn init() Keys(K, V) {
            return Keys(K, V){};
        }

        /// Checks if the key type is compatible with the database.
        /// Eg. must be a fixed size and compatible with the std.random
        /// integer methods used to generate a unique key. At the moment,
        /// only {usize} is supported.
        pub fn checkType() void {
            switch (@typeInfo(K)) {
                .int, .comptime_int => {},
                else => @compileError("Key must be an integer"),
            }
        }

        /// Check if key exists in the array already.
        pub fn existsKey(self: *Keys(K, V), parent: *Database(K, V), key: K) bool {
            _ = self;
            std.debug.print("Keys.existsKey\n", .{});
            return parent.values_map.contains(key);
        }

        /// Creates unique IDs for new documents added to the database. This
        /// function does not get used if the user specifies IDs when
        /// inserting documents into the database.
        /// If the field {_key} exists on type {V} then it will use that value
        /// as long as it is not zero.
        /// Returns the new ID of type {K}.
        pub fn uniqueKey(self: *Keys(K, V), parent: *Database(K, V), value: *V) Error!K {
            std.debug.print("Keys.uniqueKey\n", .{});
            if (@hasField(V, "_key") and value._key == 0) {
                return value._key;
            }
            var key: K = 0;
            while (key == 0 or self.existsKey(parent, key)) {
                key = parent.prng.random().int(K);
            }
            if (@hasField(V, "_key")) {
                value._key = key;
            }
            return key;
        }
    };
}
