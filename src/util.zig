const std = @import("std");

const Error = @import("./error.zig").Error;

pub fn ensureAbsolute(path: []const u8) Error![]const u8 {
    if (!std.fs.path.isAbsolute(path)) {
        const cwdPath = std.fs.cwd().realpathAlloc(std.heap.page_allocator, ".") catch {
            return Error.EnsureAbsolutePathCwd;
        };
        return std.fs.path.join(std.heap.page_allocator, &[_][]const u8{ cwdPath, path }) catch {
            return Error.EnsureAbsolutePathJoin;
        };
    }
    return path;
}

pub fn openFile(path: []const u8) Error!std.fs.File {
    const absolute_path = try ensureAbsolute(path);
    std.debug.print("open file: {s}\n", .{absolute_path});
    if (std.fs.openFileAbsolute(absolute_path, .{ .mode = .read_write, .lock = .none })) |file| {
        return file;
    } else |err| {
        if (err == std.fs.File.OpenError.FileNotFound) {
            const dropFile = std.fs.createFileAbsolute(absolute_path, .{ .truncate = false }) catch {
                return Error.CreateFile;
            };
            dropFile.close();
            return std.fs.openFileAbsolute(absolute_path, .{ .mode = .read_write, .lock = .none }) catch {
                return Error.OpenFile;
            };
        } else {
            return Error.OpenDatabasePath;
        }
    }
}

pub fn prngAlloc() !std.Random.Xoshiro256 {
    const prng = std.Random.Xoshiro256.init(blk: {
        var seed: usize = @intCast(std.time.timestamp());
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    return prng;
}

pub fn randomIntAlloc(comptime T: type) !T {
    const prng = try prngAlloc();
    var random = prng.random();
    return random.int(T);
}

pub fn randomStringAlloc(length: u8) ![]const u8 {
    const prng = try prngAlloc();
    return randomString(prng, length);
}

pub const CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
pub fn randomString(
    allocator: std.mem.Allocator,
    prng: *std.Random.Xoshiro256,
    length: u8,
) ![]const u8 {
    var str: []u8 = try allocator.alloc(u8, length);
    var strLength: usize = 0;
    const charLength = CHARS.len;
    while (strLength < length) {
        const charIndex = prng.random().intRangeAtMost(u8, 0, charLength);
        const char = CHARS[charIndex];
        str[strLength] = char;
        strLength += 1;
    }
    return str;
}
