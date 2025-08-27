const std = @import("std");

const options = @import("./options.zig");
const util = @import("./util.zig");
const Error = @import("./error.zig").Error;
const KeyValue = @import("./serialize.zig").KeyValue;
const results = @import("./results.zig");
const Keys = @import("./keys.zig").Keys;
const Hooks = @import("./hooks.zig").Hooks;
const FieldNames = @import("./field_names.zig").FieldNames;
const UpdateType = @import("./update_type.zig").UpdateType;

pub fn DataField(V: type) type {
    return struct {
        const Self: type = DataField(V);
        list: std.array_list.Managed(V),
        pub fn init(allocator: std.mem.Allocator) Self {
            const list = std.array_list.Managed(V).init(allocator);
            return Self{ .list = list };
        }
        pub fn append(self: *Self, value: V) !void {
            self.list.append(value);
        }
    };
}

fn FieldsType(V: type) type {
    const sfields = @typeInfo(V).@"struct".fields;
    const sfields_len = sfields.len;
    comptime var fields: [sfields_len]std.builtin.Type.StructField = undefined;
    comptime var i = 0;
    inline for (@typeInfo(V).@"struct".fields) |field| {
        //std.debug.print("field: {any}\n", .{field});
        const struct_field = std.builtin.Type.StructField{
            .name = field.name,
            .type = DataField(field.type),
            .default_value_ptr = &DataField(field.type).init(std.heap.page_allocator),
            .is_comptime = false,
            .alignment = @alignOf(DataField(field.type)),
        };
        fields[i] = struct_field;
        i += 1;
    }
    return @Type(
        .{
            .@"struct" = .{
                .layout = .auto,
                .fields = &fields,
                .decls = &.{},
                .is_tuple = false,
            },
        },
    );
}

pub fn Database(K: type, V: type) type {

    // Ensure key is an appropriate type before continuing.
    // This will throw a compile error if the key
    // is not an integer (which is needed for )
    Keys(K, V).checkType();

    return struct {
        const Self: type = Database(K, V);
        const KeysType = Keys(K, V);
        const HooksType = Hooks(K, V);
        const KeyValueType = KeyValue(K, V);
        const AutoHashMapType = std.AutoHashMap(K, *V);
        const DataFieldType = DataField(V);

        //
        const InsertOneResult = results.insertOneResult(K);

        //
        allocator: std.mem.Allocator,
        file: ?std.fs.File = null,
        mmap: ?[*]u8 = null,
        values_array: std.ArrayList(KeyValueType),
        values_map: AutoHashMapType,
        prng: std.Random.Xoshiro256,
        keys: KeysType,
        hooks: HooksType,

        //
        write_position: usize,
        file_size: usize,

        // compile-time fields
        created_field: bool,
        accessed_field: bool,
        modified_field: bool,
        key_field: bool,

        // fields, expose list of fields from value V
        fields: DataFieldType,

        ///
        fn init(allocator: std.mem.Allocator) !Self {
            std.debug.print("init\n", .{});

            // Open file path to database (if path is specified, not null).

            const created_field = @hasField(V, "_created");
            const accessed_field = @hasField(V, "_accessed");
            const modified_field = @hasField(V, "_modified");
            const key_field = @hasField(V, "_key");

            return Self{
                .allocator = allocator,
                .file = null,
                .mmap = null,
                .values_array = try std.ArrayList(KeyValueType).initCapacity(allocator, 0),
                .values_map = AutoHashMapType.init(allocator),
                .prng = try util.prngAlloc(),
                .keys = KeysType.init(),
                .hooks = HooksType.init(),
                .fields = DataFieldType.init(allocator),
                .created_field = created_field,
                .accessed_field = accessed_field,
                .modified_field = modified_field,
                .key_field = key_field,
                .write_position = 0,
                .file_size = 0,
            };
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
                    .current_position = 0,
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
                .{ key, self.values_map.count() },
            );
            try self.values_map.put(key, value);
            const keyValue = KeyValueType{
                .key = key,
                .value = value,
                .current_position = 0,
            };
            try self.values_array.append(self.allocator, keyValue);
            std.debug.print(
                "insertOneMemory: key: {any}, count (after): {d}\n",
                .{ key, self.values_map.count() },
            );
        }

        /// Runs when a document is inserted, udpated, upserted, or removed.
        /// If fields {_modified} or {_created} exist, they will be updated
        /// as appropriate. If this is an insertion and the values are non-zero
        /// then they will not be updated.
        fn updateValue(update: UpdateType, value: *V) void {
            switch (update) {
                UpdateType.Insert => {
                    if (@hasField(V, "_created") and value._created == 0) {
                        value._created = 1;
                    }
                    if (@hasField(V, "_modified") and value._modified == 0) {
                        value._modified = 1;
                    }
                },
                UpdateType.Remove => {},
                UpdateType.Update => {},
                UpdateType.Upsert => {},
            }
        }

        /// Inserts a document to the database with a unique ID. If the database
        /// is file-backed, it will write the entry to the disk as a serialized
        /// {SerializeValue}.
        /// Returns {InsertOneResult}.
        pub fn insertOne(self: *Self, value: *V) Error!InsertOneResult {
            std.debug.print("insertOne: key: {any}\n", .{K});
            Self.updateValue(UpdateType.Insert, value);
            const key = try self.keys.uniqueKey(self, value);
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
            return self.values_map.get(key);
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
        pub fn load(self: *Self, path: []const u8) !void {
            std.debug.print("load() \n", .{});
            self.file = try Self.openFile(path);
            if (self.file == null) {
                return Error.OpenFile;
            }
            const file = self.file orelse unreachable;
            std.debug.print("load() file found\n", .{});
            self.file_size = (try file.stat()).size;
            const result = std.os.linux.mmap(
                self.mmap,
                self.file_size,
                1 | 2,
                .{
                    .TYPE = std.os.linux.MAP_TYPE.SHARED,
                    //.POPULATE = true,
                    //.NONBLOCK = true,
                    //.HUGETLB = true,
                    //.SYNC = true,
                },
                file.handle,
                0,
            );
            std.debug.print("Load result: {d} {any}\n", .{ result, self.mmap });
            if (self.mmap) |mmap| {
                std.debug.print("load() mmap found\n", .{});
                var current_position: usize = 0;
                const mmap_ptr = @as([*]u8, @ptrCast(mmap));
                while (current_position < self.file_size) : (current_position += @sizeOf(KeyValueType)) {
                    const buffer = mmap_ptr[current_position .. current_position + @sizeOf(KeyValueType)];
                    const serialized_value = KeyValueType.deserialize(buffer, current_position);
                    std.debug.print("Deserialized one: {s}\n", .{serialized_value.value.message});
                    try self.insertOneMemory(serialized_value.key, serialized_value.value);
                }
            }
        }

        /// Returns the number of items currently held in the database.
        pub fn count(self: *Self) !usize {
            std.debug.print("count() \n", .{});
            return self.values_map.count();
        }

        /// Cleanup memory when done. Also need to deallocate all values within the values
        /// hash map and array.
        pub fn deinit(self: *Self) void {
            std.debug.print("deinit() \n", .{});
            if (self.file) |file| {
                file.close();
            }
            self.values_map.deinit();
            self.values_array.deinit(self.allocator);
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
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator);
    try db.load(path);
    db.deinit();
}

test "open database insert_one with disk" {
    const path = try prepDatabasePath();
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator);
    try db.load(path);
    var example = try ExampleStruct.randomMessage();
    const result = try db.insertOne(&example);
    try std.testing.expect(result.key > 0);
    try std.testing.expect(result.bytesWritten > 0);
    try std.testing.expect(result.success);
    db.deinit();
}

// zig test -femit-docs --test-filter "open database insert_one with disk and load" src/database.zig
test "open database insert_one with disk and load" {
    const path = try prepDatabasePath();
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator);
    try db.load(path);
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

    var db2 = try Database(u32, ExampleStruct).init(std.heap.page_allocator);
    try db2.load(path);
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
    const path = try prepDatabasePath();
    var db = try Database(u32, ExampleStruct).init(std.heap.page_allocator);
    try db.load(path);
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
