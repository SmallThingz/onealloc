const std = @import("std");
pub const SerializationFunctions = @import("serialization_functions.zig");
const SF = SerializationFunctions;

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

pub fn ToMergedT(context: SF.Context) type {
  const T = context.options.T;
  @setEvalBranchQuota(1000_000);
  return switch (@typeInfo(T)) {
    .type, .noreturn, .comptime_int, .comptime_float, .undefined, .@"fn", .frame, .@"anyframe", .enum_literal => {
      @compileError("Type '" ++ @tagName(std.meta.activeTag(@typeInfo(T))) ++ "' is not mergeable\n");
    },
    .void, .bool, .int, .float, .vector, .error_set, .null, .@"enum" => SF.GetDirectMergedT(context),
    .pointer => |pi| switch (pi.size) {
      .many, .c => if (context.options.serialize_unknown_pointer_as_usize) SF.GetDirectMergedT(context) else {
        @compileError(@tagName(pi.size) ++ " pointer cannot be serialized for type " ++ @typeName(T) ++ ", consider setting serialize_many_pointer_as_usize to true\n");
      },
      .one => switch (@typeInfo(pi.child)) {
        .@"opaque" => if (@hasDecl(pi.child, "Underlying") and @TypeOf(pi.child.Underlying) == SF.MergedSignature) pi.child else {
          @compileError("A non-mergeable opaque " ++ @typeName(pi.child) ++ " was provided to `ToMergedT`\n");
        },
        else => SF.GetPointerMergedT(context),
      },
      .slice => SF.GetSliceMergedT(context),
    },
    .array => SF.GetArrayMergedT(context),
    .@"struct" => SF.GetStructMergedT(context),
    .optional => SF.GetOptionalMergedT(context),
    .error_union => SF.GetErrorUnionMergedT(context),
    .@"union" => SF.GetUnionMergedT(context),
    .@"opaque" => if (@hasDecl(T, "Underlying") and @TypeOf(T.Underlying) == SF.MergedSignature) T else {
      @compileError("A non-mergeable opaque " ++ @typeName(T) ++ " was provided to `ToMergedT`\n");
    },
  };
}

