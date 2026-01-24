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
  /// The alignment of dynamic data, should be used by the Wrappers only
  _align: std.mem.Alignment = .@"1",
};

pub const Dynamic = Mem(.@"1");

/// A no-op opaque type that is used for static types (types with no dynamic / allocated data)
pub fn GetDirectMergedT(context: Context) type {
  const T = context.Type;
  return opaque {
    pub const Underlying = MergedSignature {.T = T};
    pub const STATIC = true; // Allow others to see if their child is static. This is required in slices
    pub inline fn write(noalias _: *T, noalias _: *Dynamic) void {}
    pub inline fn addDynamicSize(noalias _: *const T, noalias _: *usize) void {}
    pub inline fn repointer(noalias _: *T, noalias _: *Dynamic) void {}
  };
}

/// Converts a supplied pointer type to writable opaque. We change the pointer to point to the new memory for the pointed-to value
pub fn GetPointerMergedT(context: Context) type {
  if (!context.options.depointer) return GetDirectMergedT(context);

  const T = context.Type;
  const pi = @typeInfo(T).pointer;
  std.debug.assert(pi.size == .one);

  const Retval = opaque {
    pub const Underlying = MergedSignature {.T = T, ._align = .fromByteUnits(pi.alignment)};
    const Child = next_context.T(pi.child).merge();
    const next_context = context.see(T, @This());

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      var ogptr = @intFromPtr(dynamic.ptr);
      const aligned_dynamic = dynamic.alignForward(pi.alignment);
      const child_static: meta.NonConstPointer(T, .one) = @ptrCast(aligned_dynamic.ptr);
      child_static.* = val.*.*;
      dynamic.* = aligned_dynamic.from(@sizeOf(pi.child));
      Child.write(child_static, dynamic);

      if (builtin.mode == .Debug) {
        addDynamicSize(val, &ogptr);
        std.debug.assert(@intFromPtr(dynamic.ptr) == ogptr);
      }

      val.* = child_static; // TODO: figure out if this is ok
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      size.* = std.mem.alignForward(usize, size.*, pi.alignment);
      size += @sizeOf(pi.child);
      Child.addDynamicSize(val.*, size);
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) usize {
      val.* = @ptrCast(dynamic.ptr); // TODO: figure out if this is ok
      Child.repointer(val.*, dynamic.from(@sizeOf(pi.child)));
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetSliceMergedT(context: Context) type {
  if (!context.options.deslice) return GetDirectMergedT(context);

  const T = context.Type;
  const pi = @typeInfo(T).pointer;
  std.debug.assert(pi.size == .slice);

  const Retval = opaque {
    pub const Underlying = MergedSignature{.T = T, ._align = .fromByteUnits(pi.alignment)};
    const Child = next_context.T(pi.child).merge();
    const SubStatic = @hasDecl(Child, "STATIC") and Child.STATIC;
    const next_context = context.see(T, @This());

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      var ogptr = @intFromPtr(dynamic.ptr);
      const aligned_dynamic = dynamic.alignForward(pi.alignment);
      const child_static: meta.NonConstPointer(T, .slice) = blk: {
        const child_static_ptr: meta.NonConstPointer(T, .many) = @ptrCast(aligned_dynamic.ptr);
        break :blk child_static_ptr[0..val.*.len];
      };
      // We can't write the dynamic data before static data as we would need to get the size of dynamic data first. Would is prettie inefficient
      @memcpy(child_static, val.*);
      dynamic.* = aligned_dynamic.from(@sizeOf(pi.child) * child_static.len);
      if (!SubStatic) {
        for (child_static) |*elem| Child.write(elem, dynamic);
      }

      if (builtin.mode == .Debug) {
        Child.addDynamicSize(&child_static, &ogptr);
        std.debug.assert(@intFromPtr(dynamic.ptr) == ogptr);
      }

      val.*.ptr = @ptrCast(dynamic.ptr); // TODO: figure out if this is ok
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      size.* = std.mem.alignForward(usize, size.*, pi.alignment);
      size.* += @sizeOf(pi.child) * val.*.len;
      if (!SubStatic) {
        for (val.*) |*elem| Child.addDynamicSize(elem, size);
      }
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) void {
      val.*.ptr = @ptrCast(dynamic.ptr); // TODO: figure out if this is ok
      if (!SubStatic) {
        var child_dynamic = dynamic.from(@sizeOf(pi.child) * val.*.len);
        for (val.*) |*elem| Child.repointer(elem, &child_dynamic);
      }
    }
  };

  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  return Retval;
}

pub fn GetArrayMergedT(context: Context) type {
  const T = context.Type;
  @setEvalBranchQuota(1000_000);
  const ai = @typeInfo(T).array;
  // No need to .see(T) here because array children are not indirected
  const Child = context.T(ai.child).merge();

  // If the child has no dynamic data, the entire array is static.
  // We can treat it as a no-op
  if (@hasDecl(Child, "STATIC") and Child.STATIC) return GetDirectMergedT(context);

  return opaque {
    pub const Underlying = MergedSignature{.T = T, ._align = Child.Underlying._align};

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      @setEvalBranchQuota(1000_000);
      inline for (val) |*elem| Child.write(elem, dynamic);
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      @setEvalBranchQuota(1000_000);
      inline for (val) |*elem| Child.addDynamicSize(elem, size);
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) void {
      @setEvalBranchQuota(1000_000);
      inline for (val) |*elem| Child.repointer(elem, dynamic);
    }
  };
}

pub fn GetStructMergedT(context: Context) type {
  const T = context.Type;
  @setEvalBranchQuota(1000_000);
  if (!context.options.recurse) return GetDirectMergedT(context);

  const si = @typeInfo(T).@"struct";
  const ProcessedField = struct {
    original: std.builtin.Type.StructField,
    merged: type,
  };

  const Retval = opaque {
    pub const Underlying = MergedSignature{.T = T, ._align = if (STATIC) .@"1" else sorted_dynamic_fields[0].merged.Underlying._align};
    const STATIC = dynamic_field_count == 0;
    const next_context = context.see(T, @This());

    const fields = blk: {
      @setEvalBranchQuota(1000_000);
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
      @setEvalBranchQuota(1000_000);
      var dyn_count: usize = 0;
      for (fields) |f| {
        if (@hasDecl(f.merged, "STATIC") and f.merged.STATIC) continue;
        dyn_count += 1;
      }
      break :blk dyn_count;
    };

    /// The field with max alignment requirement for dynamic data is in first place
    const sorted_dynamic_fields = blk: {
      @setEvalBranchQuota(1000_000);
      var dyn_fields: [dynamic_field_count]ProcessedField = &.{};
      var i: usize = 0;
      for (fields) |f| {
        if (@hasDecl(f.merged, "STATIC") and f.merged.STATIC) continue;
        dyn_fields[i] = f;
        i += 1;
      }

      std.mem.sortContext(0, dyn_fields.len, struct {
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

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      @setEvalBranchQuota(1000_000);
      var ogptr = @intFromPtr(dynamic.ptr);
      inline for (sorted_dynamic_fields) |f| {
        f.merged.write(&@field(val, f.original.name), dynamic);
      }

      if (builtin.mode == .Debug) {
        addDynamicSize(val, &ogptr);
        std.debug.assert(@intFromPtr(dynamic.ptr) == ogptr);
      }
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      @setEvalBranchQuota(1000_000);
      inline for (sorted_dynamic_fields) |f| {
        f.merged.addDynamicSize(&@field(val, f.original.name), size);
      }
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) void {
      @setEvalBranchQuota(1000_000);
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
  const T = context.Type;
  const oi = @typeInfo(T).optional;
  const Child = context.T(oi.child).merge();
  if (@hasDecl(Child, "STATIC") and Child.STATIC) return GetDirectMergedT(context);

  return opaque {
    pub const Underlying = MergedSignature{.T = T, ._align = Child.Underlying._align};

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      if (val.* != null) {
        var ogptr = @intFromPtr(dynamic.ptr);
        Child.write(&(val.*.?), dynamic);

        if (builtin.mode == .Debug) {
          addDynamicSize(&(val.*.?), &ogptr);
          std.debug.assert(@intFromPtr(dynamic.ptr) == ogptr);
        }
      }
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      if (val.* != null) Child.addDynamicSize(&(val.*.?), size);
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) void {
      if (val.* != null) Child.repointer(&(val.*.?), dynamic);
    }
  };
}

pub fn GetErrorUnionMergedT(context: Context) type {
  const T = context.Type;
  const ei = @typeInfo(T).error_union;
  const Payload = ei.payload;

  const Child = context.T(Payload).merge();
  if (@hasDecl(Child, "STATIC") and Child.STATIC) return GetDirectMergedT(context);

  return opaque {
    pub const Underlying = MergedSignature{.T = T, ._align = Payload.Underlying._align};

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      if (val.*) |*payload_val| Child.write(payload_val, dynamic);
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      if (val.*) |*payload_val| Child.addDynamicSize(payload_val, size);
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) void {
      if (val.*) |*payload_val| Child.repointer(payload_val, dynamic);
    }
  };
}

pub fn GetUnionMergedT(context: Context) type {
  const T = context.Type;
  if (!context.options.recurse) return GetDirectMergedT(context);
  const ui = @typeInfo(T).@"union";
  const Retval = opaque {
    pub const Underlying = MergedSignature{.T = T, ._align = blk: {
      var min = @intFromEnum(fields[0].merged.Underlying._align);
      for (fields[1..]) |f| min = @min(min, @intFromEnum(f.merged.Underlying._align));
      break :blk @enumFromInt(min);
    }};
    const TagType = ui.tag_type orelse @compileError("Union '" ++ @typeName(T) ++ "' has no tag type");
    const next_context = context.see(T, @This());

    const ProcessedField = struct {
      original: std.builtin.Type.UnionField,
      merged: type,
    };

    const fields = blk: {
      var pfields: [ui.fields.len]ProcessedField = undefined;
      for (ui.fields, 0..) |f, i| pfields[i] = .{
        .original = f,
        .merged = next_context.T(f.type).merge(),
      };
      break :blk pfields;
    };

    pub fn write(noalias val: *T, noalias dynamic: *Dynamic) void {
      const active_tag = std.meta.activeTag(val.*);

      inline for (fields) |f| {
        const field_as_tag = comptime std.meta.stringToEnum(TagType, f.original.name);
        if (field_as_tag == active_tag) f.merged.write(&@field(val, f.original.name), dynamic);
      }
      unreachable; // Should never heppen
    }

    pub fn addDynamicSize(noalias val: *const T, noalias size: *usize) void {
      const active_tag = std.meta.activeTag(val.*);

      inline for (fields) |f| {
        const field_as_tag = comptime std.meta.stringToEnum(TagType, f.original.name);
        if (field_as_tag == active_tag) f.merged.addDynamicSize(&@field(val, f.original.name), size);
      }
    }

    pub fn repointer(noalias val: *T, noalias dynamic: Dynamic) void {
      const active_tag = std.meta.activeTag(val.*);

      inline for (fields) |f| {
        const field_as_tag = comptime std.meta.stringToEnum(TagType, f.original.name);
        if (field_as_tag == active_tag) f.merged.repointer(&@field(val, f.original.name), dynamic);
      }
    }
  };

  if (Retval.STATIC) return GetDirectMergedT(context);
  if (Retval.next_context.seen_recursive >= 0) return context.result_types[Retval.next_context.seen_recursive];
  if (ui.tag_type == null) @compileError("Cannot merge untagged union with dynamic data: " ++ @typeName(T));
  return Retval;
}
