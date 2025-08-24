const std = @import("std");

const options = @import("./options.zig");
const util = @import("./util.zig");
const Error = @import("./error.zig").Error;
const KeyValue = @import("./serialize.zig").KeyValue;
const results = @import("./results.zig");
const Keys = @import("./keys.zig").Keys;
const Hooks = @import("./hooks.zig").Hooks;

/// Checks if the key type is compatible with the database.
/// Eg. must be a fixed size and compatible with the std.random
/// integer methods used to generate a unique key. At the moment,
/// only {usize} is supported.
fn checkKeyType(comptime K: type) void {
    if (@TypeOf(K) != @TypeOf(usize)) {
        @compileError("Invalid key type. Choose from usize.");
    }
}

pub fn Database(comptime K: type, comptime V: type) type {

    // Ensure key is an appropriate type before continuing.
    checkKeyType(K);

    return struct {
        const Self: type = Database(K, V);
        const KeysType = Keys(K, V);
        const HooksType = Hooks(K, V);
        const KeyValueType: type = KeyValue(K, V);
        const AutoHashMapType = std.AutoHashMap(K, *V);

        //
        const InsertOneResult = results.insertOneResult(K);

        //
        allocator: std.mem.Allocator,
        file: ?std.fs.File = null,
        valuesArray: std.ArrayList(KeyValueType),
        valuesMap: AutoHashMapType,
        prng: std.Random.Xoshiro256,
        keys: KeysType,
        hooks: HooksType,

        ///
        fn init(allocator: std.mem.Allocator, path: ?[]const u8) !Self {
            std.debug.print("init\n", .{});

            // Open file path to database (if path is specified, not null).
            const file = try Self.openFile(path);

            var db = Self{
                .allocator = allocator,
                .file = file,
                .valuesArray = try std.ArrayList(KeyValueType).initCapacity(allocator, 0),
                .valuesMap = AutoHashMapType.init(allocator),
                .prng = try util.prngAlloc(),
                .keys = KeysType.init(),
                .hooks = HooksType.init(),
            };

            if (file) |fileMapped| {
                try db.load(allocator, fileMapped);
            }

            return db;
        }

        /// Open file function to avoid "must be const or comptime" errors.
        fn openFile(path: ?[]const u8) !?std.fs.File {
            if (path) |pathMapped| {
                return try util.openFile(pathMapped);
            }
            return null;
        }

        /// Write values to disk if a path was specified when the struct
        /// was created.
        pub fn writeDisk(self: *Self, key: K, value: *V) Error!usize {
            var bytesWritten: usize = 0;
            if (self.file) |file| {
                var keyValue: KeyValueType = KeyValueType{
                    .key = key,
                    .value = value,
                };
                const bytes = try keyValue.serialize(self.allocator);
                bytesWritten = file.write(bytes) catch {
                    return Error.InsertOneDiskWrite;
                };
                std.debug.print(
                    "insertOneWithKey: bytesWritten: {any}, key: {any}\n",
                    .{ bytesWritten, key },
                );
            }
            return bytesWritten;
        }

        /// Called from insert functions. This function only adds the key and value
        /// into the internal values hash map and array list.
        /// Returns {InsertOneResult}.
        pub fn insertOneMemory(self: *Self, key: K, value: *V) Error!void {
            std.debug.print(
                "insertOneMemory: key: {any}, count (before): {d}\n",
                .{ key, self.valuesMap.count() },
            );
            try self.valuesMap.put(key, value);
            const keyValue = KeyValueType{ .key = key, .value = value };
            try self.valuesArray.append(self.allocator, keyValue);
            std.debug.print(
                "insertOneMemory: key: {any}, count (after): {d}\n",
                .{ key, self.valuesMap.count() },
            );
        }

        /// Inserts a document to the database with a unique ID. If the database
        /// is file-backed, it will write the entry to the disk as a serialized
        /// {SerializeValue}.
        /// Returns {InsertOneResult}.
        pub fn insertOne(self: *Self, value: *V) Error!InsertOneResult {
            std.debug.print("insertOne: key: {any}\n", .{K});
            const key = try self.keys.uniqueKey(self);
            const bytesWritten = try self.writeDisk(key, value);
            try self.insertOneMemory(key, value);
            return InsertOneResult{
                .bytesWritten = bytesWritten,
                .success = true,
                .key = key,
            };
        }

        /// Find an item by key.
        pub fn findOneWithKey(self: *Self, key: K) Error!?*V {
            std.debug.print("findOneWithKey: key: {any}\n", .{key});
            return self.valuesMap.get(key);
        }

        /// Inserts a document to the database with the specified ID. If the
        /// database is file-backed, it will write the entry to the disk as a
        /// serialized {KeyValue}. If the ID already exists in the database,
        /// it will return {Error.InsertIdAlreadyExists}.
        /// Returns {InsertOneResult}.
        pub fn insertOneWithKey(self: *Self, key: K, value: V) Error!InsertOneResult {
            std.debug.print(
                "insertOneWithKey: key: {any} value: ${any}\n",
                .{ key, value },
            );
            if (self.keys.existsKey(self, key)) {
                return Error.KeyExists;
            }
            const bytesWritten = try self.writeDisk(key, value);
            try self.insertOneMemory(key, &value);
            return InsertOneResult{
                .bytesWritten = bytesWritten,
                .success = true,
                .key = key,
            };
        }

        ///
        pub fn load(self: *Self, allocator: std.mem.Allocator, file: std.fs.File) !void {
            std.debug.print("load() \n", .{});
            var currentPosition: usize = 0;
            const buffer = try allocator.alloc(u8, @sizeOf(KeyValueType));
            while (try file.read(buffer) > 0) : (currentPosition += @sizeOf(KeyValueType)) {
                const serializedValue = KeyValueType.deserialize(buffer);
                try self.insertOneMemory(serializedValue.key, serializedValue.value);
            }
        }

        /// Returns the number of items currently held in the database.
        pub fn count(self: *Self) !usize {
            std.debug.print("count() \n", .{});
            return self.valuesMap.count();
        }

        /// Cleanup memory when done. Also need to deallocate all values within the values
        /// hash map and array.
        pub fn deinit(self: *Self) void {
            std.debug.print("deinit() \n", .{});
            if (self.file) |file| {
                file.close();
            }
            self.valuesMap.deinit();
            self.valuesArray.deinit(self.allocator);
        }
    };
}

const ExampleStruct = struct {
    message: *const [20]u8,
    read: bool,
    fn randomMessage() !ExampleStruct {
        var prng = try util.prngAlloc();
        const message = try util.randomString(std.heap.page_allocator, &prng, 20);
        return ExampleStruct{ .message = message[0..20], .read = false };
    }
};

fn prepDatabasePath() ![]const u8 {
    const path = "/tmp/test.db";
    _ = std.fs.deleteFileAbsolute(path) catch {};
    return path;
}

test "open database only" {
    const path = try prepDatabasePath();
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator, path);
    db.deinit();
}

test "open database insert_one with disk" {
    const path = try prepDatabasePath();
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator, path);
    var example = try ExampleStruct.randomMessage();
    const result = try db.insertOne(&example);
    try std.testing.expect(result.key > 0);
    try std.testing.expect(result.bytesWritten > 0);
    try std.testing.expect(result.success);
    db.deinit();
}

test "open database insert_one with disk and load" {
    const path = try prepDatabasePath();
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator, path);
    var example = try ExampleStruct.randomMessage();
    const result = try db.insertOne(&example);
    try std.testing.expect(result.key > 0);
    try std.testing.expect(result.bytesWritten > 0);
    try std.testing.expect(result.success);
    if (try db.findOneWithKey(result.key)) |find| {
        try std.testing.expect(find.message == example.message);
    } else {
        return Error.TestFailed;
    }
    db.deinit();

    var db2 = try Database(u32, ExampleStruct).init(std.heap.page_allocator, path);
    var example2 = try ExampleStruct.randomMessage();
    const result2 = try db2.insertOne(&example2);
    try std.testing.expect(result2.key > 0);
    try std.testing.expect(result2.bytesWritten > 0);
    try std.testing.expect(result2.success);
    const count2 = try db2.count();
    try std.testing.expect(count2 == 2);

    if (try db.findOneWithKey(result.key)) |find| {
        try std.testing.expect(find.message == example.message);
    } else {
        return Error.TestFailed;
    }

    db2.deinit();
}

test "open database insert_one without disk" {
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator, null);
    var example = try ExampleStruct.randomMessage();
    const result = try db.insertOne(&example);
    try std.testing.expect(result.key > 0);
    try std.testing.expect(result.bytesWritten == 0);
    try std.testing.expect(result.success);
    const example2 = try ExampleStruct.randomMessage();
    const result2 = try db.insertOne(&example2);
    try std.testing.expect(result2.key > 0);
    try std.testing.expect(result2.bytesWritten == 0);
    try std.testing.expect(result2.success);
    db.deinit();
}
