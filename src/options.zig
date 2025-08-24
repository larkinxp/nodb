const std = @import("std");

const format = @import("./format.zig");

pub const Options = struct {
    format: format.Format,
};
