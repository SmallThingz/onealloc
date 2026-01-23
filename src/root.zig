const std = @import("std");
const builtin = @import("builtin");
const meta = @import("meta.zig");

const Bytes = meta.Bytes;
const MergedSignature = meta.MergedSignature;
pub const Context = meta.GetContext(ToMergedOptions);

/// Options to control how merging of a type is performed
pub const ToMergedOptions = struct {
  /// The type that is to be merged
  T: type,
  /// Recurse into structs and unions
  recurse: bool = true,
  /// Whether to dereference pointers or use them by value
  dereference: bool = true,
  /// What is the maximum number of expansion of slices that can be done
  /// for example in a recursive structure or nested slices
  ///
  /// eg.
  /// If we have [][]u8, and deslice = 1, we will write pointer+size of all the strings in this slice
  /// If we have [][]u8, and deslice = 2, we will write all the characters in this block
  deslice: comptime_int = 1024,
  /// Error if deslice = 0
  error_on_0_deslice: bool = true,
  /// Allow for recursive re-referencing, eg. (A has ?*A), (A has ?*B, B has ?*A), etc.
  /// When this is false and the type is recursive, compilation will error
  allow_recursive_rereferencing: bool = false,
  /// Serialize unknown pointers (C / Many / opaque pointers) as usize
  serialize_unknown_pointer_as_usize: bool = false,
  /// Only allow for safe conversions (eg: compileError on trying to merge dynamic union, dynamic error union and dynamic optional)
  error_on_unsafe_conversion: bool = false,

  /// the level of logging that is enabled
  log_level: meta.LogLevel = .none,
};

/// We take in a type and just use its byte representation to store into bits.
/// Zero-sized types ares supported and take up no space at all
pub fn GetDirectMergedT(context: Context) type {
  const T = context.options.T;
  return opaque {
    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(.@"1"),
      .static_size = @sizeOf(T),
      .alignment = context.align_hint orelse .fromByteUnits(@alignOf(T)),
    };

    pub fn write(val: *const T, static: S, _: Signature.D) usize {
      if (@bitSizeOf(T) != 0) @memcpy(static.slice(Signature.static_size), std.mem.asBytes(val));
      return 0;
    }
  };
}

/// Convert a supplid pointer type to writable opaque
pub fn GetPointerMergedT(context: Context) type {
  const T = context.options.T;
  if (!context.options.dereference) return GetDirectMergedT(context);

  const is_optional = @typeInfo(T) == .optional;
  const pi = @typeInfo(if (is_optional) @typeInfo(T).optional.child else T).pointer;
  std.debug.assert(pi.size == .one);

  const Pointer = GetDirectMergedT(context);

  const Retval = opaque {
    const next_context = context.realign(.fromByteUnits(pi.alignment)).see(T, @This());
    const Child = next_context.T(pi.child).merge();

    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(if (is_optional) .@"1" else Child.Signature.alignment),
      .static_size = Pointer.Signature.static_size,
      .alignment = context.align_hint orelse .fromByteUnits(@alignOf(T)),
    };

    pub fn write(val: *const T, static: S, _dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Pointer.Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(_dynamic.ptr), Signature.D.alignment));

      if (is_optional and val.* == null) {
        std.debug.assert(0 == Pointer.write(&@as(T, null), static, undefined));
        return 0;
      }
      const dynamic = if (is_optional) _dynamic.alignForward(Child.Signature.alignment) else _dynamic;
      const dptr: T = @ptrCast(@alignCast(dynamic.ptr));
      std.debug.assert(0 == Pointer.write(&dptr, static, undefined));

      const child_static = dynamic.till(Child.Signature.static_size);
      // Align 1 if child is static, so no issue here, static and dynamic children an be written by same logic
      const child_dynamic = dynamic.from(Child.Signature.static_size).alignForward(.fromByteUnits(Child.Signature.D.alignment));
      const written = Child.write(if (is_optional) val.*.? else val.*, child_static, child_dynamic);

      if (std.meta.hasFn(Child, "getDynamicSize") and builtin.mode == .Debug) {
        std.debug.assert(written == Child.getDynamicSize(if (is_optional) val.*.? else val.*, @intFromPtr(child_dynamic.ptr)) - @intFromPtr(child_dynamic.ptr));
      }
      return written + @intFromPtr(child_dynamic.ptr) - @intFromPtr(_dynamic.ptr);
    }

    pub fn getDynamicSize(val: *const T, size: usize) usize {
      std.debug.assert(std.mem.isAligned(size, Signature.D.alignment));
      var new_size = size;
      if (is_optional) {
        if (val.* == null) return new_size;
        new_size = std.mem.alignForward(usize, new_size, Child.Signature.alignment.toByteUnits());
      }

      new_size += Child.Signature.static_size;
      if (std.meta.hasFn(Child, "getDynamicSize")) {
        new_size = std.mem.alignForward(usize, new_size, Child.Signature.D.alignment);
        new_size = Child.getDynamicSize(if (is_optional) val.*.? else val.*, new_size);
      }

      return new_size;
    }

    pub fn repointer(static: S, _dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Pointer.Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(_dynamic.ptr), Signature.D.alignment));

      if (is_optional and @as(*T, @ptrCast(static.ptr)).* == null) return 0;
      const dynamic = if (is_optional) _dynamic.alignForward(Child.Signature.alignment) else _dynamic;
      const dptr: T = @ptrCast(@alignCast(dynamic.ptr));
      std.debug.assert(0 == Pointer.write(&dptr, static, undefined));
      if (!std.meta.hasFn(Child, "repointer")) {
        return Child.Signature.static_size + @intFromPtr(dynamic.ptr) - @intFromPtr(_dynamic.ptr);
      }

      const child_static = dynamic.till(Child.Signature.static_size);
      // Align 1 if child is static, so no issue here, static and dynamic children an be written by same logic
      const child_dynamic = dynamic.from(Child.Signature.static_size).alignForward(.fromByteUnits(Child.Signature.D.alignment));
      const written = Child.repointer(child_static, child_dynamic);

      if (std.meta.hasFn(Child, "getDynamicSize") and builtin.mode == .Debug) {
        const val = @as(*T, @ptrCast(static.ptr)).*;
        std.debug.assert(written == Child.getDynamicSize(if (is_optional) val.? else val, @intFromPtr(child_dynamic.ptr)) - @intFromPtr(child_dynamic.ptr));
      }

      return written + @intFromPtr(child_dynamic.ptr) - @intFromPtr(_dynamic.ptr);
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetSliceMergedT(context: Context) type {
  const T = context.options.T;
  if (context.options.deslice == 0) {
    if (context.options.error_on_0_deslice) {
      @compileError("Cannot deslice type " ++ @typeName(T) ++ " any further as options.deslice is 0");
    }
    return GetDirectMergedT(context);
  }

  const is_optional = @typeInfo(T) == .optional;
  const pi = @typeInfo(if (is_optional) @typeInfo(T).optional.child else T).pointer;
  std.debug.assert(pi.size == .slice);
  const Slice = GetDirectMergedT(context);

  const Retval = opaque {
    const next_context = context.realign(.fromByteUnits(pi.alignment)).see(T, @This());
    const next_options = blk: {
      var retval = context.options;
      if (next_context.seen_recursive == -1) retval.deslice -= 1;
      break :blk retval;
    };
    const Child = next_context.reop(next_options).T(pi.child).merge();
    const SubStatic = !std.meta.hasFn(Child, "getDynamicSize");
    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(if (is_optional) .@"1" else Child.Signature.alignment),
      .static_size = Slice.Signature.static_size,
      .alignment = Slice.Signature.alignment,
    };

    pub fn write(val: *const T, static: S, _dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(_dynamic.ptr), Signature.D.alignment));
      const dynamic = if (is_optional) _dynamic.alignForward(Child.Signature.alignment) else _dynamic;

      var header_to_write = val.*;
      if (!is_optional or val.* != null) header_to_write.ptr = @ptrCast(dynamic.ptr);
      std.debug.assert(0 == Slice.write(&header_to_write, static, undefined));
      if (is_optional and val.* == null) return 0;
      const slice = if (is_optional) val.*.? else val.*;

      const len = slice.len;
      if (Child.Signature.static_size == 0 or len == 0) return 0;

      var child_static = dynamic.till(Child.Signature.static_size * len);
      var child_dynamic = dynamic.from(Child.Signature.static_size * len).alignForward(.fromByteUnits(Child.Signature.D.alignment));

      for (slice) |*item| {
        if (!SubStatic) child_dynamic = child_dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.write(item, child_static, if (SubStatic) undefined else child_dynamic);

        if (!SubStatic and builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(item, @intFromPtr(child_dynamic.ptr) - @intFromPtr(child_dynamic.ptr)));
        }

        child_static = child_static.from(Child.Signature.static_size).assertAligned(Child.Signature.alignment);
        if (!SubStatic) child_dynamic = child_dynamic.from(written);
      }

      return @intFromPtr(child_dynamic.ptr) - @intFromPtr(_dynamic.ptr);
    }

    pub const getDynamicSize = if (Child.Signature.static_size == 0) void else _getDynamicSize;
    fn _getDynamicSize(val: *const T, size: usize) usize {
      std.debug.assert(std.mem.isAligned(size, Signature.D.alignment));
      if (is_optional and val.* == null) return size;
      const slice = if (is_optional) val.*.? else val.*;
      var new_size = size + Child.Signature.static_size * slice.len;

      if (!SubStatic) {
        for (slice) |*item| {
          new_size = std.mem.alignForward(usize, new_size, Child.Signature.D.alignment);
          new_size = Child.getDynamicSize(item, new_size);
        }
      }

      return new_size;
    }

    pub fn repointer(static: S, _dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(_dynamic.ptr), Signature.D.alignment));
      const dynamic = if (is_optional) _dynamic.alignForward(Child.Signature.alignment) else _dynamic;

      const header: *T = @ptrCast(static.ptr);
      if (is_optional and header.* == null) return 0;
      header.*.ptr = @ptrCast(dynamic.ptr);
      const len = header.*.len;

      if (Child.Signature.static_size == 0 or len == 0) return 0;
      if (SubStatic) return Child.Signature.static_size * len;

      var child_static = dynamic.till(Child.Signature.static_size * len);
      var child_dynamic = dynamic.from(Child.Signature.static_size * len).alignForward(.fromByteUnits(Child.Signature.D.alignment));

      for (0..len) |i| {
        child_dynamic = child_dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.repointer(child_static, child_dynamic);

        if (builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(&header.*[i], @intFromPtr(child_dynamic.ptr) - @intFromPtr(child_dynamic.ptr)));
        }

        child_static = child_static.from(Child.Signature.static_size).assertAligned(Child.Signature.alignment);
        child_dynamic = child_dynamic.from(written);
      }

      return @intFromPtr(child_dynamic.ptr) - @intFromPtr(_dynamic.ptr);
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetArrayMergedT(context: Context) type {
  const T = context.options.T;
  @setEvalBranchQuota(1000_000);
  const ai = @typeInfo(T).array;
  // No need to .see(T) here as the child will handle this anyway and if the array type is repeated, the child will be too.
  const Child = context.T(ai.child).merge();

  // If the child has no dynamic data, the entire array is static.
  // We can treat it as a direct memory copy.
  if (!std.meta.hasFn(Child, "getDynamicSize")) return GetDirectMergedT(context);

  return opaque {
    const S = Bytes(Signature.alignment);

    pub const Signature = MergedSignature{
      .T = T,
      .D = Child.Signature.D,
      .static_size = Child.Signature.static_size * ai.len,
      .alignment = Child.Signature.alignment,
    };

    pub fn write(val: *const T, static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(dynamic.ptr), Signature.D.alignment));

      var child_static = static.till(Signature.static_size);
      var child_dynamic = dynamic;

      inline for (val) |*item| {
        child_dynamic = child_dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.write(item, child_static, child_dynamic);

        if (builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(item, @intFromPtr(child_dynamic.ptr)) - @intFromPtr(child_dynamic.ptr));
        }

        child_static = child_static.from(Child.Signature.static_size).assertAligned(Child.Signature.alignment);
        child_dynamic = child_dynamic.from(written);
      }

      return @intFromPtr(child_dynamic.ptr) - @intFromPtr(dynamic.ptr);
    }

    pub fn getDynamicSize(val: *const T, size: usize) usize {
      std.debug.assert(std.mem.isAligned(size, Signature.D.alignment));
      var new_size = size;
      inline for (0..ai.len) |i| {
        new_size = std.mem.alignForward(usize, new_size, Child.Signature.D.alignment);
        new_size = Child.getDynamicSize(&val[i], new_size);
      }
      return new_size;
    }

    pub fn repointer(static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(dynamic.ptr), Signature.D.alignment));

      var child_static = static.till(Signature.static_size);
      var child_dynamic = dynamic;

      inline for (0..ai.len) |i| {
        child_dynamic = child_dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.repointer(child_static, child_dynamic);

        if (builtin.mode == .Debug) {
          const val: *T = @ptrCast(static.ptr);
          std.debug.assert(written == Child.getDynamicSize(&val[i], @intFromPtr(child_dynamic.ptr)) - @intFromPtr(child_dynamic.ptr));
        }

        child_static = child_static.from(Child.Signature.static_size).assertAligned(Child.Signature.alignment);
        child_dynamic = child_dynamic.from(written);
      }

      return @intFromPtr(child_dynamic.ptr) - @intFromPtr(dynamic.ptr);
    }
  };
}

pub fn GetStructMergedT(context: Context) type {
  const T = context.options.T;
  @setEvalBranchQuota(1000_000);
  if (!context.options.recurse) return GetDirectMergedT(context);

  const si = @typeInfo(T).@"struct";
  const ProcessedField = struct {
    original: std.builtin.Type.StructField,
    merged: type,
    static_offset: comptime_int,
  };

  const Retval = opaque {
    const next_context = context.see(T, @This());

    const fields = blk: {
      var pfields: [si.fields.len]ProcessedField = undefined;

      for (si.fields, 0..) |f, i| {
        pfields[i] = .{
          .original = f,
          .merged = next_context.realign(if (si.layout == .@"packed") .@"1" else .fromByteUnits(f.alignment)).T(f.type).merge(),
          .static_offset = @offsetOf(T, f.name),
        };
      }

      std.sort.pdqContext(0, pfields.len, struct {
        fields: []ProcessedField,

        fn greaterThan(self: @This(), lhs: usize, rhs: usize) bool {
          const ls = self.fields[lhs].merged.Signature;
          const rs = self.fields[rhs].merged.Signature;

          if (!std.meta.hasFn(self.fields[lhs].merged, "getDynamicSize")) return false;
          if (!std.meta.hasFn(self.fields[rhs].merged, "getDynamicSize")) return true;

          if (ls.D.alignment != rs.D.alignment) return ls.D.alignment > rs.D.alignment;
          if (ls.D.alignment != 1) return false;

          comptime var lst = @typeInfo(ls.T);
          comptime var rst = @typeInfo(rs.T);

          if ((lst == .optional or lst == .pointer) and (rst == .optional or rst == .pointer)) {
            if (lst == .optional) lst = @typeInfo(lst.optional.child);
            if (rst == .optional) rst = @typeInfo(rst.optional.child);
          } else if (lst == .optional or rst == .optional) {
            return lst != .optional;
          }

          if (lst == .pointer and rst == .pointer) {
            if (lst.pointer.size != rst.pointer.size) {
              const lsize = lst.pointer.size;
              const rsize = rst.pointer.size;
              if (lsize == .one) return true;
              if (rsize == .one) return false;

              if (lsize == .slice) return true;
              if (rsize == .slice) return false;

              return false;
            } else {
              return @alignOf(lst.pointer.child) > @alignOf(rst.pointer.child);
            }
          } else if (lst == .pointer or rst == .pointer) {
            if (lst == .pointer) return lst.pointer.size != .slice and @alignOf(lst.pointer.child) > @alignOf(rst.pointer.child);
            return rst.pointer.size == .slice or @alignOf(lst.pointer.child) > @alignOf(rst.pointer.child);
          }

          return false;
        }

        pub const lessThan = greaterThan;

        pub fn swap(self: @This(), lhs: usize, rhs: usize) void {
          const temp = self.fields[lhs];
          self.fields[lhs] = self.fields[rhs];
          self.fields[rhs] = temp;
        }
      }{ .fields = &pfields });

      break :blk pfields;
    };

    const FirstNonStaticT = blk: {
      for (fields, 0..) |f, i| if (std.meta.hasFn(f.merged, "getDynamicSize")) break :blk i;
      break :blk fields.len;
    };

    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(.fromByteUnits(fields[FirstNonStaticT].merged.Signature.D.alignment)),
      .static_size = @sizeOf(T),
      .alignment = context.align_hint orelse .fromByteUnits(@alignOf(T)),
    };

    pub fn write(val: *const T, static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(dynamic.ptr), Signature.D.alignment));

      var dynamic_offset: usize = 0;
      inline for (fields) |f| {
        const child_static = static.from(f.static_offset).assertAligned(f.merged.Signature.alignment);

        if (!std.meta.hasFn(f.merged, "getDynamicSize")) {
          const written = f.merged.write(&@field(val.*, f.original.name), child_static, undefined);
          std.debug.assert(written == 0);
        } else {
          const misaligned_dynamic = dynamic.from(dynamic_offset);
          const aligned_dynamic = misaligned_dynamic.alignForward(.fromByteUnits(f.merged.Signature.D.alignment));
          const written = f.merged.write(&@field(val.*, f.original.name), child_static, aligned_dynamic);

          if (builtin.mode == .Debug) {
            std.debug.assert(written == f.merged.getDynamicSize(&@field(val.*, f.original.name), @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
          }

          dynamic_offset += written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(misaligned_dynamic.ptr);
        }
      }

      return dynamic_offset;
    }

    pub fn getDynamicSize(val: *const T, size: usize) usize {
      std.debug.assert(std.mem.isAligned(size, Signature.D.alignment));
      var new_size: usize = size;

      inline for (fields) |f| {
        if (!std.meta.hasFn(f.merged, "getDynamicSize")) continue;
        new_size = std.mem.alignForward(usize, new_size, f.merged.Signature.D.alignment);
        new_size = f.merged.getDynamicSize(&@field(val.*, f.original.name), new_size);
      }

      return new_size;
    }

    pub fn repointer(static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      std.debug.assert(std.mem.isAligned(@intFromPtr(dynamic.ptr), Signature.D.alignment));

      var dynamic_offset: usize = 0;
      inline for (fields) |f| {
        const child_static = static.from(f.static_offset).assertAligned(f.merged.Signature.alignment);

        if (std.meta.hasFn(f.merged, "getDynamicSize")) {
          const misaligned_dynamic = dynamic.from(dynamic_offset);
          const aligned_dynamic = misaligned_dynamic.alignForward(.fromByteUnits(f.merged.Signature.D.alignment));
          const written = f.merged.repointer(child_static, aligned_dynamic);

          if (builtin.mode == .Debug) {
            const val: *T = @ptrCast(static.ptr);
            std.debug.assert(written == f.merged.getDynamicSize(&@field(val.*, f.original.name), @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
          }

          dynamic_offset += written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(misaligned_dynamic.ptr);
        }
      }

      return dynamic_offset;
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  if (Retval.FirstNonStaticT == Retval.fields.len) return GetDirectMergedT(context);
  if (si.layout == .@"packed") @compileError("Packed structs with dynamic data are not supported");
  return Retval;
}

pub fn GetOptionalMergedT(context: Context) type {
  const T = context.options.T;
  const oi = @typeInfo(T).optional;
  const Child = context.T(oi.child).merge();
  if (!std.meta.hasFn(Child, "getDynamicSize")) return GetDirectMergedT(context);

  if (context.options.error_on_unsafe_conversion) {
    @compileError("Cannot merge unsafe optional type " ++ @typeName(T));
  }

  const Tag = context.T(bool).merge();

  const alignment = context.align_hint orelse .fromByteUnits(@alignOf(T));
  return opaque {
    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(.@"1"),
      .static_size = @sizeOf(T),
      .alignment = alignment,
    };

    pub fn write(val: *const T, static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      const tag_static = static.from(Child.Signature.static_size).assertAligned(Child.Signature.alignment);
      const child_static = static.till(Child.Signature.static_size);

      if (val.*) |*payload_val| {
        std.debug.assert(0 == Tag.write(&@as(bool, true), tag_static, undefined));
        const aligned_dynamic = dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.write(payload_val, child_static, aligned_dynamic);

        if (builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(payload_val, @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
        }

        return written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(dynamic.ptr);
      } else {
        std.debug.assert(0 == Tag.write(&@as(bool, false), tag_static, undefined));
        return 0;
      }
    }

    pub fn getDynamicSize(val: *const T, size: usize) usize {
      if (val.*) |*payload_val| {
        const new_size = std.mem.alignForward(usize, size, Child.Signature.D.alignment);
        return Child.getDynamicSize(payload_val, new_size);
      } else {
        return size;
      }
    }

    pub fn repointer(static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      const child_static = static.till(Child.Signature.static_size);

      const val: *T = @ptrCast(static.ptr);
      if (val.*) |*payload_val| {
        const aligned_dynamic = dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.repointer(child_static, aligned_dynamic);

        if (builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(payload_val, @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
        }

        return written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(dynamic.ptr);
      }
      return 0;
    }
  };
}

pub fn GetErrorUnionMergedT(context: Context) type {
  const T = context.options.T;
  const ei = @typeInfo(T).error_union;
  const Payload = ei.payload;
  const ErrorSet = ei.error_set;
  const ErrorInt = std.meta.Int(.unsigned, @bitSizeOf(ErrorSet));

  const Child = context.T(Payload).merge();
  if (!std.meta.hasFn(Child, "getDynamicSize")) return GetDirectMergedT(context);

  if (context.options.error_on_unsafe_conversion) {
    @compileError("Cannot merge unsafe error union type " ++ @typeName(T));
  }

  const Err = context.T(ErrorInt).merge();

  const ErrSize = Err.Signature.static_size;
  const PayloadSize = Child.Signature.static_size;
  const PayloadBeforeError = PayloadSize >= ErrSize;
  const UnionSize = if (PayloadSize < ErrSize) 2 * ErrSize
    else if (PayloadSize <= 16) 2 * PayloadSize
    else PayloadSize + 16;

  return opaque {
    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(.@"1"),
      .static_size = UnionSize,
      .alignment = std.mem.Alignment.max(Child.Signature.alignment, Err.Signature.alignment),
    };

    pub fn write(val: *const T, static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));

      const payload_buffer = if (PayloadBeforeError) static.till(PayloadSize) else static.from(ErrSize);
      const error_buffer = if (PayloadBeforeError) static.from(PayloadSize) else static.till(ErrSize);

      if (val.*) |*payload_val| {
        std.debug.assert(0 == Err.write(&@as(ErrorInt, 0), error_buffer, undefined));
        const aligned_dynamic = dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.write(payload_val, payload_buffer, aligned_dynamic);

        if (builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(payload_val, @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
        }

        return written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(dynamic.ptr);
      } else |err| {
        const error_int: ErrorInt = @intFromError(err);
        std.debug.assert(0 == Err.write(&error_int, error_buffer, undefined));
        return 0;
      }
    }

    pub fn getDynamicSize(val: *const T, size: usize) usize {
      if (val.*) |*payload_val| {
        const new_size = std.mem.alignForward(usize, size, Child.Signature.D.alignment);
        return Child.getDynamicSize(payload_val, new_size);
      } else {
        return size;
      }
    }

    pub fn repointer(static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));

      const payload_buffer = if (PayloadBeforeError) static.till(PayloadSize) else static.from(ErrSize);
      const val: *T = @ptrCast(static.ptr);

      if (val.*) |*payload_val| {
        const aligned_dynamic = dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        const written = Child.repointer(payload_buffer, aligned_dynamic);

        if (builtin.mode == .Debug) {
          std.debug.assert(written == Child.getDynamicSize(payload_val, @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
        }

        return written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(dynamic.ptr);
      }
    }
  };
}

pub fn GetUnionMergedT(context: Context) type {
  const T = context.options.T;
  if (!context.options.recurse) return GetDirectMergedT(context);

  const ui = @typeInfo(T).@"union";
  const TagType = ui.tag_type orelse std.meta.FieldEnum(T);
  const Retval = opaque {
    const next_context = context.see(T, @This());

    const ProcessedField = struct {
      original: std.builtin.Type.UnionField,
      merged: type,
    };

    const fields = blk: {
      var pfields: [ui.fields.len]ProcessedField = undefined;
      for (ui.fields, 0..) |f, i| {
        if (f.alignment < @alignOf(f.type)) {
          @compileError("Underaligned union fields cause memory corruption!\n"); // https://github.com/ziglang/zig/issues/19404, https://github.com/ziglang/zig/issues/21343
        }
        pfields[i] = .{
          .original = f,
          .merged = next_context.realign(.fromByteUnits(f.alignment)).T(f.type).merge(),
        };
      }
      break :blk pfields;
    };

    const Tag = context.realign(null).T(TagType).merge();
    const max_child_static_size = blk: {
      var max_size: usize = 0;
      for (fields) |f| max_size = @max(max_size, f.merged.Signature.static_size);
      break :blk max_size;
    };

    const max_child_static_alignment = blk: {
      var max_align: u29 = 1;
      for (fields) |f| max_align = @max(max_align, f.merged.Signature.alignment.toByteUnits());
      break :blk max_align;
    };

    const alignment: std.mem.Alignment = context.align_hint orelse .fromByteUnits(@alignOf(T));
    const tag_first = Tag.Signature.alignment.toByteUnits() > max_child_static_alignment;

    const S = Bytes(Signature.alignment);
    pub const Signature = MergedSignature{
      .T = T,
      .D = Bytes(.@"1"),
      .static_size = @sizeOf(T),
      .alignment = alignment,
    };

    pub fn write(val: *const T, static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      const active_tag = std.meta.activeTag(val.*);
      if (tag_first) {
        std.debug.assert(0 == Tag.write(&active_tag, static.till(Tag.Signature.static_size), undefined));
      } else {
        std.debug.assert(0 == Tag.write(&active_tag, static.from(max_child_static_size), undefined));
      }
      // we dont need to align static again since if the tag is first,
      // it had greater alignment and hence static data is aligned already

      inline for (fields) |f| {
        const field_as_tag = comptime std.meta.stringToEnum(TagType, f.original.name);
        if (field_as_tag == active_tag) {
          const child_static = if (tag_first) static.from(max_child_static_size).assertAligned(f.merged.Signature.alignment)
            else static.till(f.merged.Signature.static_size).assertAligned(f.merged.Signature.alignment);

          if (!std.meta.hasFn(f.merged, "getDynamicSize")) {
            return f.merged.write(&@field(val.*, f.original.name), child_static, undefined);
          } else {
            const aligned_dynamic = dynamic.alignForward(.fromByteUnits(f.merged.Signature.D.alignment));
            const written = f.merged.write(&@field(val.*, f.original.name), child_static, aligned_dynamic);

            if (builtin.mode == .Debug) {
              std.debug.assert(written == f.merged.getDynamicSize(&@field(val.*, f.original.name), @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
            }

            return written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(dynamic.ptr);
          }
        }
      }
      unreachable; // Should never heppen
    }

    pub fn getDynamicSize(val: *const T, size: usize) usize {
      std.debug.assert(std.mem.isAligned(size, Signature.D.alignment));
      const active_tag = std.meta.activeTag(val.*);

      inline for (fields) |f| {
        const field_as_tag = comptime std.meta.stringToEnum(TagType, f.original.name);
        if (field_as_tag == active_tag) {
          if (!std.meta.hasFn(f.merged, "getDynamicSize")) return size;
          const new_size = std.mem.alignForward(usize, size, f.merged.Signature.D.alignment);
          return f.merged.getDynamicSize(&@field(val.*, f.original.name), new_size);
        }
      }
      unreachable;
    }

    pub fn repointer(static: S, dynamic: Signature.D) usize {
      std.debug.assert(std.mem.isAligned(@intFromPtr(static.ptr), Signature.alignment.toByteUnits()));
      const val: *T = @ptrCast(static.ptr);
      const active_tag = std.meta.activeTag(val.*);
      // we dont need to align static again since if the tag is first,
      // it had greater alignment and hence static data is aligned already

      inline for (fields) |f| {
        const field_as_tag = comptime std.meta.stringToEnum(TagType, f.original.name);
        if (field_as_tag == active_tag) {
          const child_static = if (tag_first) static.from(max_child_static_size).assertAligned(f.merged.Signature.alignment)
            else static.till(f.merged.Signature.static_size).assertAligned(f.merged.Signature.alignment);

          if (std.meta.hasFn(f.merged, "getDynamicSize")) {
            const aligned_dynamic = dynamic.alignForward(.fromByteUnits(f.merged.Signature.D.alignment));
            const written = f.merged.repointer(child_static, aligned_dynamic);

            if (builtin.mode == .Debug) {
              std.debug.assert(written == f.merged.getDynamicSize(&@field(val.*, f.original.name), @intFromPtr(aligned_dynamic.ptr)) - @intFromPtr(aligned_dynamic.ptr));
            }

            return written + @intFromPtr(aligned_dynamic.ptr) - @intFromPtr(dynamic.ptr);
          } else {
            return 0;
          }
        }
      }
      unreachable; // Should never heppen
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];

  if (comptime blk: {
    for (Retval.fields) |f| {
      if (std.meta.hasFn(f.merged, "getDynamicSize")) {
        break :blk false;
      }
    }
    break :blk true;
  }) return GetDirectMergedT(context);

  if (ui.tag_type == null) {
    @compileError("Cannot merge untagged union " ++ @typeName(T));
  }

  if (context.options.error_on_unsafe_conversion) {
    @compileError("Cannot merge unsafe union type " ++ @typeName(T));
  }

  return Retval;
}

pub fn ToMergedT(context: Context) type {
  const T = context.options.T;
  @setEvalBranchQuota(1000_000);
  return switch (@typeInfo(T)) {
    .type, .noreturn, .comptime_int, .comptime_float, .undefined, .@"fn", .frame, .@"anyframe", .enum_literal => {
      @compileError("Type '" ++ @tagName(std.meta.activeTag(@typeInfo(T))) ++ "' is not mergeable\n");
    },
    .void, .bool, .int, .float, .vector, .error_set, .null => GetDirectMergedT(context),
    .pointer => |pi| switch (pi.size) {
      .many, .c => if (context.options.serialize_unknown_pointer_as_usize) GetDirectMergedT(context) else {
        @compileError(@tagName(pi.size) ++ " pointer cannot be serialized for type " ++ @typeName(T) ++ ", consider setting serialize_many_pointer_as_usize to true\n");
      },
      .one => switch (@typeInfo(pi.child)) {
        .@"opaque" => if (@hasDecl(pi.child, "Signature") and @TypeOf(pi.child.Signature) == MergedSignature) pi.child
          else if (context.options.error_on_unsafe_conversion) GetDirectMergedT(context) else {
          @compileError("A non-mergeable opaque " ++ @typeName(pi.child) ++ " was provided to `ToMergedT`\n");
        },
        else => GetPointerMergedT(context),
      },
      .slice => GetSliceMergedT(context),
    },
    .array => GetArrayMergedT(context),
    .@"struct" => GetStructMergedT(context),
    .optional => |oi| switch (@typeInfo(oi.child)) {
      .pointer => |pi| switch (pi.size) {
        .many, .c => if (context.options.serialize_unknown_pointer_as_usize) GetDirectMergedT(context) else {
          @compileError(@tagName(pi.size) ++ " pointer cannot be serialized for type " ++ @typeName(T) ++ ", consider setting serialize_many_pointer_as_usize to true\n");
        },
        .one => GetPointerMergedT(context),
        .slice => GetSliceMergedT(context),
      },
      else => GetOptionalMergedT(context),
    },
    .error_union => GetErrorUnionMergedT(context),
    .@"enum" => GetDirectMergedT(context),
    .@"union" => GetUnionMergedT(context),
    .@"opaque" => if (@hasDecl(T, "Signature") and @TypeOf(T.Signature) == MergedSignature) T else {
      @compileError("A non-mergeable opaque " ++ @typeName(T) ++ " was provided to `ToMergedT`\n");
    },
  };
}

/// A generic wrapper that manages the memory for a merged object.
pub fn WrapConverted(MergedT: type) type {
  const T = MergedT.Signature.T;
  return struct {
    pub const Underlying = MergedT;
    memory: []align(MergedT.Signature.alignment.toByteUnits()) u8,

    /// Returns the total size that would be required to store this value
    /// Expects there to be no data cycles
    pub fn getSize(value: *const T) usize {
      const static_size = MergedT.Signature.static_size;
      return if (@hasDecl(MergedT, "getDynamicSize")) MergedT.getDynamicSize(value, static_size) else static_size;
    }

    /// Allocates memory and merges the initial value into a self-managed buffer.
    /// The Wrapper instance owns the memory and must be de-initialized with `deinit`.
    /// Expects there to be no data cycles
    pub fn init(allocator: std.mem.Allocator, value: *const T) !@This() {
      const memory = try allocator.alignedAlloc(u8, MergedT.Signature.alignment.toByteUnits(), getSize(value));
      var retval: @This() = .{ .memory = memory };
      retval.setAssert(value);
      return retval;
    }

    /// Frees the memory owned by the Wrapper.
    pub fn deinit(self: *const @This(), allocator: std.mem.Allocator) void {
      allocator.free(self.memory);
    }

    /// Returns a mutable pointer to the merged data, allowing modification.
    /// The pointer is valid as long as the Wrapper is not de-initialized.
    pub fn get(self: *const @This()) *T {
      return @as(*T, @ptrCast(self.memory.ptr));
    }

    /// Creates a new, independent Wrapper containing a deep copy of the data.
    pub fn clone(self: *const @This(), allocator: std.mem.Allocator) !@This() {
      return try @This().init(allocator, self.get());
    }

    /// Set a new value into the wrapper. Invalidates any references to the old value
    /// Expects there to be no data cycles
    pub fn set(self: *@This(), allocator: std.mem.Allocator, value: *const T) !void {
      const memory = try allocator.realloc(self.memory, getSize(value));
      self.memory = memory;
      return self.setAssert(value);
    }

    /// Set a new value into the wrapper, asserting that underlying allocation can hold it. Invalidates any references to the old value
    /// Expects there to be no data cycles
    pub fn setAssert(self: *@This(), value: *const T) void {
      if (builtin.mode == .Debug) { // debug.assert alone may does not be optimized out
        std.debug.assert(getSize(value) <= self.memory.len);
      }
      const dynamic_buffer = MergedT.Signature.D.init(self.memory[MergedT.Signature.static_size..]).alignForward(.fromByteUnits(MergedT.Signature.D.alignment));
      const written = MergedT.write(value, .initAssert(self.memory[0..MergedT.Signature.static_size]), dynamic_buffer);

      if (builtin.mode == .Debug) {
        std.debug.assert(written + @intFromPtr(dynamic_buffer.ptr) - @intFromPtr(self.memory.ptr) == getSize(value));
      }
    }

    /// Updates the internal pointers within the merged data structure. This is necessary
    /// if the underlying `memory` buffer is moved (e.g., after a memcpy).
    pub fn repointer(self: *@This()) void {
      if (!std.meta.hasFn(MergedT, "getDynamicSize")) return; // Static data, no updation needed

      const static_size = MergedT.Signature.static_size;
      if (static_size == 0) return;

      const dynamic_from = std.mem.alignForward(usize, static_size, MergedT.Signature.D.alignment);
      const written = MergedT.repointer(.initAssert(self.memory[0..static_size]), .initAssert(self.memory[dynamic_from..]));

      if (builtin.mode == .Debug) {
        std.debug.assert(written + dynamic_from == getSize(self.get()));
      }
    }
  };
}

pub fn Wrapper(options: ToMergedOptions) type {
  return WrapConverted(Context.init(options, ToMergedT));
}
