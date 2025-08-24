const std = @import("std");

const Database = @import("./database.zig").Database;
const Error = @import("./error.zig").Error;

pub fn Keys(comptime K: type, comptime V: type) type {
    return struct {
        /// ?
        pub fn init() Keys(K, V) {
            return Keys(K, V){};
        }

        /// Check if key exists in the array already.
        pub fn existsKey(self: *Keys(K, V), parent: *Database(K, V), key: K) bool {
            _ = self;
            std.debug.print("Keys.existsKey\n", .{});
            return parent.valuesMap.contains(key);
        }

        /// Creates unique IDs for new documents added to the database. This
        /// function does not get used if the user specifies IDs when
        /// inserting documents into the database.
        /// Returns the new ID of type {K}.
        pub fn uniqueKey(self: *Keys(K, V), parent: *Database(K, V)) Error!K {
            std.debug.print("Keys.uniqueKey\n", .{});
            var key: K = 0;
            while (key == 0 or self.existsKey(parent, key)) {
                key = parent.prng.random().int(K);
            }
            return key;
        }
    };
}
