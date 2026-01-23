const std = @import("std");

const SerializationFunctions = @import("serialization_functions.zig");

/// Options to control how merging of a type is performed
pub const MergeOptions = struct {
  /// The type that is to be merged
  T: type,
  /// Recurse into structs and unions
  recurse: bool = true,
  /// Whether to dereference pointers or use them by value
  depointer: bool = true,
  /// What is the maximum number of expansion of slices that can be done
  /// for example in a recursive structure or nested slices
  ///
  /// eg.
  /// If we have val: []u8, and deslice = false, we will write only val.ptr + val.len
  /// If we have val: []u8, and deslice = true, we will write all the characters in this block as well as val.ptr + val.len
  /// Nested deslicing is also supported. For example, if we have val: [][]u8, and deslice = true, we will write all the characters
  ///   + list of pointers & lengths (pointers will point to slice where the strings are stored) + the top pointer & length
  deslice: bool = true,
  /// Serialize unknown pointers (C / Many / opaque pointers) as usize. This makes the data non-movable and thus is disabled by default.
  serialize_unknown_pointer_as_usize: bool = false,
};


