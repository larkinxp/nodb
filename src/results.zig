pub fn insertOneResult(comptime K: type) type {
    return struct {
        key: K,
        bytesWritten: usize,
        success: bool,
    };
}
