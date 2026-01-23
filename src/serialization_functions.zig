const std = @import("std");
const builtin = @import("builtin");
const meta = @import("meta.zig");
const root = @import("root.zig");

const Mem = meta.Mem;
const MergeOptions = root.MergeOptions;
pub const Context = meta.GetContext(MergeOptions);

/// This is used to recognize if types were returned by ToMerged.
/// This is done by assigning `pub const Underlying = MergedSignature;` inside an opaque
pub const MergedSignature = struct {
  /// The underlying type that was transformed
  T: type,
  /// The type of dynamic data that will be written to by the child
  D: type,
};

/// We take in a type and just use its byte representation to store into bits.
/// Zero-sized types ares supported and take up no space at all
pub fn GetDirectMergedT(context: Context) type {
  const T = context.options.T;
  return opaque {
    pub const Underlying = MergedSignature {.T = T, .D = Mem(.@"1")};
    pub const STATIC = true; // Allow others to see if their child is static. This is required in slices
    pub inline fn write(noalias _: *T, noalias _: *Underlying.D) void { return 0; }
    pub inline fn addDynamicSize(noalias _: *const T, noalias _: *usize) void { return; }
    pub inline fn repointer(noalias _: *T, noalias _: *Underlying.D) void { return 0; }
  };
}

/// Convert a supplied pointer type to writable opaque
pub fn GetPointerMergedT(context: Context) type {
  if (!context.options.depointer) return GetDirectMergedT(context);

  const T = context.options.T;
  const is_optional = @typeInfo(T) == .optional;
  const pi = @typeInfo(if (is_optional) @typeInfo(T).optional.child else T).pointer;
  std.debug.assert(pi.size == .one);

  const Retval = opaque {
    pub const Underlying = MergedSignature {.T = T, .D = Mem(if (is_optional) .@"1" else .fromByteUnits(pi.alignment))};
    const next_context = context.see(T, @This());
    const Child = next_context.T(pi.child).merge();

    pub fn write(noalias val: *T, noalias _dynamic: *Underlying.D) void {
      if (comptime is_optional) {
        if (val.* == null) return 0;
      }
      const dynamic = if (is_optional) _dynamic.alignForward(.fromByteUnits(pi.alignment)) else _dynamic.*;
      const child_static: *pi.child = @ptrCast(dynamic.ptr);
      val.* = child_static; // TODO: figure out if this is ok
      var child_dynamic: Child.Underlying.D = dynamic.from(@sizeOf(pi.child)).alignForward(.fromByteUnits(Child.Underlying.D.alignment));
      Child.write(child_static, &child_dynamic);
      _dynamic.* = if (is_optional) child_dynamic.from(0) else child_dynamic;

      if (builtin.mode == .Debug) {
        var ogptr = @intFromPtr(dynamic.from(@sizeOf(pi.child)).alignForward(.fromByteUnits(Child.Underlying.D.alignment)).ptr);
        Child.addDynamicSize(child_static, &ogptr);
        std.debug.assert(@intFromPtr(child_dynamic.ptr) == ogptr);
      }
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      std.debug.assert(std.mem.isAligned(size, Underlying.D.alignment));
      if (comptime is_optional) {
        if (val.* == null) return;
        size.* = std.mem.alignForward(usize, size.*, Underlying.D.alignment);
      }
      Child.addDynamicSize(if (is_optional) val.*.? else val.*, size);
    }

    pub fn repointer(noalias val: *T, noalias _dynamic: *Underlying.D) usize {
      if (comptime is_optional) {
        if (val.* == null) return 0;
      }
      const dynamic = if (is_optional) _dynamic.alignForward(.fromByteUnits(pi.alignment)) else _dynamic.*;
      const child_static: *pi.child = @ptrCast(dynamic.ptr);
      val.* = child_static; // TODO: figure out if this is ok
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetSliceMergedT(context: Context) type {
  if (!context.options.deslice) return GetDirectMergedT(context);

  const T = context.options.T;
  const is_optional = @typeInfo(T) == .optional;
  const pi = @typeInfo(if (is_optional) @typeInfo(T).optional.child else T).pointer;
  std.debug.assert(pi.size == .slice);

  const Retval = opaque {
    const next_context = context.realign(.fromByteUnits(pi.alignment)).see(T, @This());
    const Child = next_context.T(pi.child).merge();
    const SubStatic = !std.meta.hasFn(Child, "getDynamicSize");
    pub const Underlying = MergedSignature{.T = T, .D = Mem(if (is_optional) .@"1" else .fromByteUnits(pi.alignment))};

    pub fn write(noalias val: *T, noalias _dynamic: *Underlying.D) void {
      if (comptime is_optional) {
        if (val.* == null) return 0;
      }

      const dynamic = if (is_optional) _dynamic.alignForward(.fromByteUnits(pi.alignment)) else _dynamic.*;
      const og_val = if (is_optional) val.*.? else val.*;
      (if (is_optional) val.*.? else val.*).ptr = @ptrCast(dynamic.ptr); // TODO: figure out if this is ok
      var child_dynamic = dynamic.from(@sizeOf(pi.child) * og_val.len).alignForward(.fromByteUnits(Child.Underlying.D.alignment));
      @memcpy((if (is_optional) val.*.? else val.*), og_val);
      for (if (is_optional) val.*.? else val.*) |*elem| Child.write(elem, &child_dynamic);

      if (builtin.mode == .Debug) {
        var ogptr = @intFromPtr(dynamic.from(@sizeOf(pi.child) * og_val.len).alignForward(.fromByteUnits(Child.Underlying.D.alignment)).ptr);
        Child.addDynamicSize(val.*, &ogptr);
        std.debug.assert(@intFromPtr(child_dynamic.ptr) == ogptr);
      }
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      if (comptime is_optional) {
        if (val.* == null) return 0;
        size.* = std.mem.alignForward(usize, size.*, Underlying.D.alignment);
      }

      if (comptime !(@hasDecl(Child, "STATIC") and Child.STATIC)) {
        for (if (is_optional) val.*.? else val.*) |*elem| {
          Child.addDynamicSize(elem, size);
        }
      }
    }

    pub fn repointer(noalias val: *T, noalias _dynamic: *Underlying.D) void {
      if (comptime is_optional) {
        if (val.* == null) return 0;
      }

      const dynamic = if (is_optional) _dynamic.alignForward(.fromByteUnits(pi.alignment)) else _dynamic.*;
      const len = (if (is_optional) val.*.? else val.*).len;
      (if (is_optional) val.*.? else val.*).ptr = @ptrCast(dynamic.ptr); // TODO: figure out if this is ok
      var child_dynamic = dynamic.from(@sizeOf(pi.child) * len).alignForward(.fromByteUnits(Child.Underlying.D.alignment));
      for (if (is_optional) val.*.? else val.*) |*elem| Child.repointer(elem, &child_dynamic);
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetArrayMergedT(context: Context) type {
  const T = context.options.T;
  @setEvalBranchQuota(1000_000);
  const ai = @typeInfo(T).array;
  // No need to .see(T) here because array children are not indirected
  const Child = context.T(ai.child).merge();

  // If the child has no dynamic data, the entire array is static.
  // We can treat it as a no-op
  if (@hasDecl(Child, "STATIC") and Child.STATIC) return GetDirectMergedT(context);

  return opaque {
    pub const Underlying = MergedSignature{.T = T, .D = Child.Underlying.D};

    pub fn write(noalias val: *T, noalias dynamic: *Underlying.D) void {
      inline for (val) |*elem| Child.write(elem, dynamic);
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      inline for (val) |*elem| Child.addDynamicSize(elem, size);
    }

    pub fn repointer(noalias val: *T, noalias dynamic: *Underlying.D) void {
      inline for (val) |*elem| Child.repointer(elem, dynamic);
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
  };

  const Retval = opaque {
    pub const Underlying = MergedSignature{.T = T, .D = Mem(.fromByteUnits(sorted_dynamic_fields[0].merged.Underlying.D.alignment))};
    pub const STATIC = dynamic_field_count == 0;
    const next_context = context.see(T, @This());

    const fields = blk: {
      var pfields: [si.fields.len]ProcessedField = undefined;
      for (si.fields, 0..) |f, i| {
        pfields[i] = .{
          .original = f,
          .merged = next_context.T(f.type).merge(),
        };
      }
      break :blk pfields;
    };

    const dynamic_field_count = blk: {
      var dyn_count: usize = 0;
      for (fields) |f| {
        if (@hasDecl(f.merged, "STATIC") and f.merged.STATIC) continue;
        dyn_count += 1;
      }
      break :blk dyn_count;
    };

    const sorted_dynamic_fields = blk: {
      var dyn_fields: [dynamic_field_count]ProcessedField = &.{};
      var i: usize = 0;
      for (fields) |f| {
        if (@hasDecl(f.merged, "STATIC") and f.merged.STATIC) continue;
        dyn_fields[i] = f;
        i += 1;
      }

      std.sort.pdqContext(0, dyn_fields.len, struct {
        fields: []ProcessedField,

        fn greaterThan(self: @This(), lhs: usize, rhs: usize) bool {
          const ls = self.fields[lhs].merged.Underlying;
          const rs = self.fields[rhs].merged.Underlying;
          if (ls.D.alignment != rs.D.alignment) return ls.D.alignment > rs.D.alignment;
          return false;
        }

        pub const lessThan = greaterThan;

        pub fn swap(self: @This(), lhs: usize, rhs: usize) void {
          const temp = self.fields[lhs];
          self.fields[lhs] = self.fields[rhs];
          self.fields[rhs] = temp;
        }
      }{ .fields = &dyn_fields });
      break :blk dyn_fields;
    };

    pub fn write(noalias val: *T, noalias dynamic: *Underlying.D) void {
      var ogptr = @intFromPtr(dynamic.ptr);
      inline for (sorted_dynamic_fields) |f| {
        f.merged.write(&@field(val, f.original.name), @ptrCast(dynamic));
      }

      if (builtin.mode == .Debug) {
        addDynamicSize(val, &ogptr);
        std.debug.assert(@intFromPtr(dynamic.ptr) == ogptr);
      }
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      std.debug.assert(std.mem.isAligned(size, Underlying.D.alignment));
      inline for (sorted_dynamic_fields) |f| {
        f.merged.addDynamicSize(&@field(val, f.original.name), size);
      }
    }

    pub fn repointer(noalias val: *T, noalias dynamic: *Underlying.D) void {
      inline for (sorted_dynamic_fields) |f| {
        f.merged.repointer(&@field(val, f.original.name), @ptrCast(dynamic));
      }
    }
  };

  if (Retval.STATIC) return GetDirectMergedT(context);
  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetOptionalMergedT(context: Context) type {
  const T = context.options.T;
  const oi = @typeInfo(T).optional;
  const Child = context.T(oi.child).merge();
  if (@hasDecl(Child, "STATIC") and Child.STATIC) return GetDirectMergedT(context);

  return opaque {
    pub const Signature = MergedSignature{.T = T, .D = Mem(.@"1")};

    pub fn write(noalias val: *T, noalias _dynamic: *Signature.D) void {
      if (val.* != null) {
        var dynamic = _dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        var ogptr = @intFromPtr(dynamic.ptr);

        Child.write(&(val.*.?), &dynamic);
        _dynamic.* = @bitCast(dynamic);

        if (builtin.mode == .Debug) {
          addDynamicSize(&(val.*.?), &ogptr);
          std.debug.assert(@intFromPtr(dynamic.ptr) == ogptr);
        }
      }
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      std.debug.assert(std.mem.isAligned(size, Signature.D.alignment));
      if (val.* != null) {
        size.* = std.mem.alignForward(usize, size.*, Child.Signature.D.alignment);
        addDynamicSize(&(val.*.?), size);
      }
    }

    pub fn repointer(noalias val: *T, noalias _dynamic: *Signature.D) void {
      if (val.* != null) {
        var dynamic = _dynamic.alignForward(.fromByteUnits(Child.Signature.D.alignment));
        Child.repointer(&(val.*.?), &dynamic);
        _dynamic.* = @bitCast(dynamic);
      }
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
        .@"opaque" => if (@hasDecl(pi.child, "Signature") and @TypeOf(pi.child.Signature) == MergedSignature) pi.child else {
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
      const memory = try allocator.alignedAlloc(u8, MergedT.Signature.alignment, getSize(value));
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

pub fn Wrapper(options: MergeOptions) type {
  return WrapConverted(Context.init(options, ToMergedT));
}
