const std = @import("std");

fn expectEqual(expected: anytype, actual: anytype) error{TestExpectedEqual}!void {
  const print = std.debug.print;

  if (std.meta.activeTag(@typeInfo(@TypeOf(actual))) != std.meta.activeTag(@typeInfo(@TypeOf(expected)))) {
    print("expected type {s}, found type {s}\n", .{ @typeName(@TypeOf(expected)), @typeName(@TypeOf(actual)) });
    return error.TestExpectedEqual;
  }

  switch (@typeInfo(@TypeOf(actual))) {
    .noreturn, .@"opaque", .frame, .@"anyframe", => @compileError("value of type " ++ @typeName(@TypeOf(actual)) ++ " encountered"),

    .void => return,

    .type => {
      if (actual != expected) {
        print("expected type {s}, found type {s}\n", .{ @typeName(expected), @typeName(actual) });
        return error.TestExpectedEqual;
      }
    },

    .bool, .int, .float, .comptime_float, .comptime_int, .enum_literal, .@"enum", .@"fn", .error_set => {
      if (actual != expected) {
        print("expected {}, found {}\n", .{ expected, actual });
        return error.TestExpectedEqual;
      }
    },

    .pointer => |pointer| {
      switch (pointer.size) {
        .one, .many, .c => {
          if (actual == expected) {
            // std.debug.dumpCurrentStackTrace(null);
            // print("pointers are same for {s}\n", .{ @typeName(@TypeOf(actual)) });
            return;
          }
          return expectEqual(actual.*, expected.*);
        },
        .slice => {
          if (actual.len != expected.len) {
            print("expected slice len {}, found {}\n", .{ expected.len, actual.len });
            print("expected: {any}\nactual: {any}\n", .{ expected, actual });
            return error.TestExpectedEqual;
          }
          if (actual.ptr == expected.ptr) {
            // std.debug.dumpCurrentStackTrace(null);
            // print("slices are same for {s}\n", .{ @typeName(@TypeOf(actual)) });
            return;
          }
          for (actual, expected, 0..) |va, ve, i| {
            expectEqual(va, ve) catch |e| {
              print("index {d} incorrect.\nexpected:: {any}\nfound:: {any}\n", .{ i, expected[i], actual[i] });
              return e;
            };
          }
        },
      }
    },

    .array => |array| {
      inline for (0..array.len) |i| {
        expectEqual(expected[i], actual[i]) catch |e| {
          print("index {d} incorrect.\nexpected:: {any}\nfound:: {any}\n", .{ i, expected[i], actual[i] });
          return e;
        };
      }
    },

    .vector => |info| {
      var i: usize = 0;
      while (i < info.len) : (i += 1) {
        if (!std.meta.eql(expected[i], actual[i])) {
          print("index {d} incorrect.\nexpected:: {any}\nfound:: {any}\n", .{ i, expected[i], actual[i] });
          return error.TestExpectedEqual;
        }
      }
    },

    .@"struct" => |structType| {
      inline for (structType.fields) |field| {
        errdefer print("field `{s}` incorrect\n", .{ field.name });
        try expectEqual(@field(expected, field.name), @field(actual, field.name));
      }
    },

    .@"union" => |union_info| {
      if (union_info.tag_type == null) @compileError("Unable to compare untagged union values for type " ++ @typeName(@TypeOf(actual)));
      const Tag = std.meta.Tag(@TypeOf(expected));
      const expectedTag = @as(Tag, expected);
      const actualTag = @as(Tag, actual);

      try expectEqual(expectedTag, actualTag);

      switch (expected) {
        inline else => |val, tag| try expectEqual(val, @field(actual, @tagName(tag))),
      }
    },

    .optional => {
      if (expected) |expected_payload| {
        if (actual) |actual_payload| {
          try expectEqual(expected_payload, actual_payload);
        } else {
          print("expected {any}, found null\n", .{expected_payload});
          return error.TestExpectedEqual;
        }
      } else {
        if (actual) |actual_payload| {
          print("expected null, found {any}\n", .{actual_payload});
          return error.TestExpectedEqual;
        }
      }
    },

    .error_union => {
      if (expected) |expected_payload| {
        if (actual) |actual_payload| {
          try expectEqual(expected_payload, actual_payload);
        } else |actual_err| {
          print("expected {any}, found {}\n", .{ expected_payload, actual_err });
          return error.TestExpectedEqual;
        }
      } else |expected_err| {
        if (actual) |actual_payload| {
          print("expected {}, found {any}\n", .{ expected_err, actual_payload });
          return error.TestExpectedEqual;
        } else |actual_err| {
          try expectEqual(expected_err, actual_err);
        }
      }
    },

    else => @compileError("Unsupported type in expectEqual: " ++ @typeName(@TypeOf(expected))),
  }
}

const testing = std.testing;
const root = @import("root.zig");
const ToMergedOptions = root.ToMergedOptions;
const Context = root.Context;
const ToMergedT = root.ToMergedT;

test {
  std.testing.refAllDeclsRecursive(@This());
  std.testing.refAllDeclsRecursive(root);
}

fn _testMergingDemerging(value: anytype, comptime options: ToMergedOptions) !void {
  const MergedT = Context.init(options, ToMergedT);
  const static_size = MergedT.Signature.static_size;
  var buffer: [static_size + 4096]u8 = undefined;

  const total_size = if (std.meta.hasFn(MergedT, "getDynamicSize")) MergedT.getDynamicSize(&value, static_size) else static_size;
  if (total_size > buffer.len) {
    std.log.err("buffer too small for test. need {d}, have {d}", .{ total_size, buffer.len });
    return error.NoSpaceLeft;
  }

  const dynamic_from = std.mem.alignForward(usize, static_size, MergedT.Signature.D.alignment);
  const written_dynamic_size = MergedT.write(&value, .initAssert(buffer[0..static_size]), .initAssert(buffer[dynamic_from..]));
  try std.testing.expectEqual(total_size - dynamic_from, written_dynamic_size);

  try expectEqual(&value, @as(*@TypeOf(value), @ptrCast(@alignCast(&buffer))));

  const copy = try testing.allocator.alignedAlloc(u8, MergedT.Signature.alignment.toByteUnits(), total_size);
  defer testing.allocator.free(copy);
  @memcpy(copy, buffer[0..total_size]);
  @memset(buffer[0..total_size], 0);

  // repointer only is non static
  if (std.meta.hasFn(MergedT, "getDynamicSize")) {
    const repointered_size = MergedT.repointer(.initAssert(copy[0..static_size]), .initAssert(copy[dynamic_from..]));
    try std.testing.expectEqual(written_dynamic_size, repointered_size);
  }

  // verify
  try expectEqual(&value, @as(*@TypeOf(value), @ptrCast(copy)));
}

fn testMerging(value: anytype) !void {
  try _testMergingDemerging(value, .{ .T = @TypeOf(value) });
}

test "primitives" {
  try testMerging(@as(u32, 42));
  try testMerging(@as(f64, 123.456));
  try testMerging(@as(bool, true));
  try testMerging(@as(void, {}));
}

test "pointers" {
  var x: u64 = 12345;
  try testMerging(&x);
  try _testMergingDemerging(&x, .{ .T = *u64, .dereference = false });
}

test "slices" {
  // primitive
  try testMerging(@as([]const u8, "hello zig"));

  // struct
  const Point = struct { x: u8, y: u8 };
  try testMerging(@as([]const Point, &.{ .{ .x = 1, .y = 2 }, .{ .x = 3, .y = 4 } }));

  // nested
  try testMerging(@as([]const []const u8, &.{"hello", "world", "zig", "rocks"}));

  // empty
  try testMerging(@as([]const u8, &.{}));
  try testMerging(@as([]const []const u8, &.{}));
  try testMerging(@as([]const []const u8, &.{"", "a", ""}));
}

test "arrays" {
  // primitive
  try testMerging([4]u8{ 1, 2, 3, 4 });

  // struct array
  const Point = struct { x: u8, y: u8 };
  try testMerging([2]Point{ .{ .x = 1, .y = 2 }, .{ .x = 3, .y = 4 } });

  // nested arrays
  try testMerging([2][2]u8{ .{ 1, 2 }, .{ 3, 4 } });

  // empty
  try testMerging([0]u8{});
}

test "structs" {
  // Simple
  const Point = struct { x: i32, y: i32 };
  try testMerging(Point{ .x = -10, .y = 20 });

  // Nested
  const Line = struct { p1: Point, p2: Point };
  try testMerging(Line{ .p1 = .{ .x = 1, .y = 2 }, .p2 = .{ .x = 3, .y = 4 } });
}

test "enums" {
  // Simple
  const Color = enum { red, green, blue };
  try testMerging(Color.green);
}

test "optional" {
  // value
  var x: ?i32 = 42;
  try testMerging(x);
  x = null;
  try testMerging(x);

  // pointer
  var y: i32 = 123;
  var opt_ptr: ?*i32 = &y;
  try testMerging(opt_ptr);

  opt_ptr = null;
  try testMerging(opt_ptr);
}

test "error_unions" {
  const MyError = error{Oops};
  var eu: MyError!u32 = 123;
  try testMerging(eu);
  eu = MyError.Oops;
  try testMerging(eu);
}

test "unions" {
  const Payload = union(enum) {
    a: u32,
    b: bool,
    c: void,
  };
  try testMerging(Payload{ .a = 99 });
  try testMerging(Payload{ .b = false });
  try testMerging(Payload{ .c = {} });
}

test "complex struct" {
  const Nested = struct {
    c: u4,
    d: bool,
  };

  const KitchenSink = struct {
    a: i32,
    b: []const u8,
    c: [2]Nested,
    d: ?*const i32,
    e: f32,
  };

  var value = KitchenSink{
    .a = -1,
    .b = "dynamic slice",
    .c = .{ .{ .c = 1, .d = true }, .{ .c = 2, .d = false } },
    .d = &@as(i32, 42),
    .e = 3.14,
  };

  try testMerging(value);

  value.b = "";
  try testMerging(value);

  value.d = null;
  try testMerging(value);
}

test "slice of complex structs" {
  const Item = struct {
    id: u64,
    name: []const u8,
    is_active: bool,
  };

  const items = [_]Item{
    .{ .id = 1, .name = "first", .is_active = true },
    .{ .id = 2, .name = "second", .is_active = false },
    .{ .id = 3, .name = "", .is_active = true },
  };

  try testMerging(items[0..]);
}

test "complex composition" {
  const Complex1 = struct {
    a: u32,
    b: u32,
    c: u32,
  };

  const Complex2 = struct {
    a: Complex1,
    b: []const Complex1,
  };

  const SuperComplex = struct {
    a: Complex1,
    b: Complex2,
    c: []const union(enum) {
      a: Complex1,
      b: Complex2,
    },
  };

  const value = SuperComplex{
    .a = .{ .a = 1, .b = 2, .c = 3 },
    .b = .{
      .a = .{ .a = 4, .b = 5, .c = 6 },
      .b = &.{.{ .a = 7, .b = 8, .c = 9 }},
    },
    .c = &.{
      .{ .a = .{ .a = 10, .b = 11, .c = 12 } },
      .{ .b = .{ .a = .{ .a = 13, .b = 14, .c = 15 }, .b = &.{.{ .a = 16, .b = 17, .c = 18 }} } },
    },
  };

  try testMerging(value);
}

test "multiple dynamic fields" {
  const MultiDynamic = struct {
    a: []const u8,
    b: i32,
    c: []const u8,
  };

  var value = MultiDynamic{
    .a = "hello",
    .b = 12345,
    .c = "world",
  };
  try testMerging(value);

  value.a = "";
  try testMerging(value);
}

test "complex array" {
  const Struct = struct {
    a: u8,
    b: u32,
  };
  const value = [2]Struct{
    .{ .a = 1, .b = 100 },
    .{ .a = 2, .b = 200 },
  };

  try testMerging(value);
}

test "packed struct with mixed alignment fields" {
  const MixedPack = packed struct {
    a: u2,
    b: u8,
    c: u32,
    d: bool,
  };

  const value = MixedPack{
    .a = 3,
    .b = 't',
    .c = 1234567,
    .d = true,
  };

  try testMerging(value);
}

test "struct with zero-sized fields" {
  const ZST_1 = struct {
    a: u32,
    b: void,
    c: [0]u8,
    d: []const u8,
    e: bool,
  };
  try testMerging(ZST_1{
    .a = 123,
    .b = {},
    .c = .{},
    .d = "non-zst",
    .e = false,
  });

  const ZST_2 = struct {
    a: u32,
    zst1: void,
    zst_array: [0]u64,
    dynamic_zst_slice: []const void,
    zst_union: union(enum) {
      z: void,
      d: u64,
    },
    e: bool,
  };

  var value_2 = ZST_2{
    .a = 123,
    .zst1 = {},
    .zst_array = .{},
    .dynamic_zst_slice = &.{ {}, {}, {} },
    .zst_union = .{ .z = {} },
    .e = true,
  };

  try testMerging(value_2);

  value_2.zst_union = .{ .d = 999 };
  try testMerging(value_2);
}

test "array of unions with dynamic fields" {
  const Message = union(enum) {
    text: []const u8,
    code: u32,
    err: void,
  };

  const messages = [3]Message{
    .{ .text = "hello" },
    .{ .code = 404 },
    .{ .text = "world" },
  };

  try testMerging(messages);
}

test "pointer and optional abuse" {
  const Point = struct { x: i32, y: i32 };
  const PointerAbuse = struct {
    a: ?*const Point,
    b: *const ?Point,
    c: ?*const ?Point,
    d: []const ?*const ?Point,
  };

  const p1: Point = .{ .x = 1, .y = 1 };
  const p2: ?Point = .{ .x = 2, .y = 2 };
  const p3: ?Point = null;

  const value = PointerAbuse{
    .a = &p1,
    .b = &p2,
    .c = &p2,
    .d = &.{ &p2, null, &p3 },
  };

  try testMerging(value);
}

test "deeply nested struct with one dynamic field at the end" {
  const Level4 = struct {
    data: []const u8,
  };
  const Level3 = struct {
    l4: Level4,
  };
  const Level2 = struct {
    l3: Level3,
    val: u64,
  };
  const Level1 = struct {
    l2: Level2,
  };

  const value = Level1{
    .l2 = .{
      .l3 = .{
        .l4 = .{
          .data = "we need to go deeper",
        },
      },
      .val = 99,
    },
  };
  try testMerging(value);
}

test "slice of structs with dynamic fields" {
  const LogEntry = struct {
    timestamp: u64,
    message: []const u8,
  };
  const entries = [_]LogEntry{
    .{ .timestamp = 1, .message = "first entry" },
    .{ .timestamp = 2, .message = "" },
    .{ .timestamp = 3, .message = "third entry has a much longer message to test buffer allocation" },
  };

  try testMerging(entries[0..]);
}

test "struct with multiple, non-contiguous dynamic fields" {
  const UserProfile = struct {
    username: []const u8,
    user_id: u64,
    bio: []const u8,
    karma: i32,
    avatar_url: []const u8,
  };

  const user = UserProfile{
    .username = "zigger",
    .user_id = 1234,
    .bio = "Loves comptime and robust software.",
    .karma = 9999,
    .avatar_url = "http://ziglang.org/logo.svg",
  };

  try testMerging(user);
}

test "union with multiple dynamic fields" {
  const Packet = union(enum) {
    message: []const u8,
    points: []const struct { x: f32, y: f32 },
    code: u32,
  };

  try testMerging(Packet{ .message = "hello world" });
  try testMerging(Packet{ .points = &.{.{ .x = 1.0, .y = 2.0 }, .{ .x = 3.0, .y = 4.0}} });
  try testMerging(Packet{ .code = 404 });
}

test "advanced zero-sized type handling" {
  const ZstContainer = struct {
    zst1: void,
    zst2: [0]u8,
    data: []const u8, // This is the only thing that should take space
  };
  try testMerging(ZstContainer{ .zst1 = {}, .zst2 = .{}, .data = "hello" });

  const ZstSliceContainer = struct {
    id: u32,
    zst_slice: []const void,
  };

  try testMerging(ZstSliceContainer{ .id = 99, .zst_slice = &.{ {}, {}, {} } });
}

test "deep optional and pointer nesting" {
  const DeepOptional = struct {
    val: ??*const u32,
  };

  const x: u32 = 123;

  // Fully valued
  try testMerging(DeepOptional{ .val = &x });

  // Inner pointer is null
  try testMerging(DeepOptional{ .val = @as(?*const u32, null) });

  // Outer optional is null
  try testMerging(DeepOptional{ .val = @as(??*const u32, null) });
}

test "recursion limit with dereference" {
  const Node = struct {
    payload: u32,
    next: ?*const @This(),
  };

  const n3 = Node{ .payload = 3, .next = null };
  const n2 = Node{ .payload = 2, .next = &n3 };
  const n1 = Node{ .payload = 1, .next = &n2 };

  // This should only serialize n1 and the pointer to n2. 
  // The `write` for n2 will hit the dereference limit and treat it as a direct (raw pointer) value.
  try _testMergingDemerging(n1, .{ .T = Node, .allow_recursive_rereferencing = true });
}

test "recursive type merging" {
  const Node = struct {
    payload: u32,
    next: ?*const @This(),
  };

  const n4 = Node{ .payload = 4, .next = undefined };
  const n3 = Node{ .payload = 3, .next = &n4 };
  const n2 = Node{ .payload = 2, .next = &n3 };
  const n1 = Node{ .payload = 1, .next = &n2 };

  try _testMergingDemerging(n1, .{ .T = Node, .allow_recursive_rereferencing = true  });
}

test "mutual recursion" {
  const Namespace = struct {
    const NodeA = struct {
      name: []const u8,
      b: ?*const NodeB,
    };
    const NodeB = struct {
      value: u32,
      a: ?*const NodeA,
    };
  };

  const NodeA = Namespace.NodeA;
  const NodeB = Namespace.NodeB;

  // Create a linked list: a1 -> b1 -> a2 -> null
  const a2 = NodeA{ .name = "a2", .b = null };
  const b1 = NodeB{ .value = 100, .a = &a2 };
  const a1 = NodeA{ .name = "a1", .b = &b1 };

  try _testMergingDemerging(a1, .{ .T = NodeA, .allow_recursive_rereferencing = true });
}

test "deeply nested, mutually recursive structures with no data cycles" {
  const Namespace = struct {
    const MegaStructureA = struct {
      id: u32,
      description: []const u8,
      next: ?*const @This(), // Direct recursion: A -> A
      child_b: *const NodeB, // Mutual recursion: A -> B
    };

    const NodeB = struct {
      value: f64,
      relatives: [2]?*const @This(), // Direct recursion: B -> [2]B
      next_a: ?*const MegaStructureA, // Mutual recursion: B -> A
      leaf: ?*const LeafNode, // Points to a simple terminal node
    };

    const LeafNode = struct {
      data: []const u8,
    };
  };

  const MegaStructureA = Namespace.MegaStructureA;
  const NodeB = Namespace.NodeB;
  const LeafNode = Namespace.LeafNode;

  const leaf1 = LeafNode{ .data = "Leaf Node One" };
  const leaf2 = LeafNode{ .data = "Leaf Node Two" };

  const b_leaf_1 = NodeB{
    .value = 1.1,
    .next_a = null,
    .relatives = .{ null, null },
    .leaf = &leaf1,
  };
  const b_leaf_2 = NodeB{
    .value = 2.2,
    .next_a = null,
    .relatives = .{ null, null },
    .leaf = &leaf2,
  };

  const a_intermediate = MegaStructureA{
    .id = 100,
    .description = "Intermediate A",
    .next = null, // Terminates this A-chain
    .child_b = &b_leaf_1,
  };

  const b_middle = NodeB{
    .value = 3.3,
    .next_a = &a_intermediate,
    .relatives = .{ &b_leaf_1, &b_leaf_2 },
    .leaf = null,
  };

  const a_before_root = MegaStructureA{
    .id = 200,
    .description = "Almost Root A",
    .next = null,
    .child_b = &b_leaf_2,
  };

  const root_node = MegaStructureA{
    .id = 1,
    .description = "The Root",
    .next = &a_before_root,
    .child_b = &b_middle,
  };

  try _testMergingDemerging(root_node, .{ .T = MegaStructureA, .allow_recursive_rereferencing = true });
}


//---
//Wrapper
//---

const Wrapper = root.Wrapper;

test "Wrapper init, get, and deinit" {
  const Point = struct { x: i32, y: []const u8 };
  var wrapped_point = try Wrapper(.{ .T = Point }).init(testing.allocator, &.{ .x = 42, .y = "hello" });
  defer wrapped_point.deinit(testing.allocator);

  const p = wrapped_point.get();
  try expectEqual(@as(i32, 42), p.x);
  try std.testing.expectEqualSlices(u8, "hello", p.y);
}

test "Wrapper clone" {
  const Data = struct { id: u32, items: []const u32 };
  var wrapped1 = try Wrapper(.{ .T = Data }).init(testing.allocator, &.{ .id = 1, .items = &.{ 10, 20, 30 } });
  defer wrapped1.deinit(testing.allocator);

  var wrapped2 = try wrapped1.clone(testing.allocator);
  defer wrapped2.deinit(testing.allocator);

  try testing.expect(wrapped1.memory.ptr != wrapped2.memory.ptr);

  const d1 = wrapped1.get();
  const d2 = wrapped2.get();
  try expectEqual(d1.id, d2.id);
  try std.testing.expectEqualSlices(u32, d1.items, d2.items);

  wrapped1.get().id = 99;
  try expectEqual(@as(u32, 99), wrapped1.get().id);
  try expectEqual(@as(u32, 1), wrapped2.get().id);
}

test "Wrapper set" {
  const Data = struct { id: u32, items: []const u32 };
  var wrapped = try Wrapper(.{ .T = Data }).init(testing.allocator, &.{ .id = 1, .items = &.{10} });
  defer wrapped.deinit(testing.allocator);

  // Set to a larger value
  try wrapped.set(testing.allocator, &.{ .id = 2, .items = &.{ 20, 30, 40 } });
  var d = wrapped.get();
  try expectEqual(@as(u32, 2), d.id);
  try std.testing.expectEqualSlices(u32, &.{ 20, 30, 40 }, d.items);
  
  // Set to a smaller value
  try wrapped.set(testing.allocator, &.{ .id = 3, .items = &.{50} });
  d = wrapped.get();
  try expectEqual(@as(u32, 3), d.id);
  try std.testing.expectEqualSlices(u32, &.{50}, d.items);
}

test "Wrapper repointer" {
  const LogEntry = struct {
    timestamp: u64,
    message: []const u8,
  };

  var wrapped = try Wrapper(.{ .T = LogEntry }).init(
    testing.allocator,
    &.{ .timestamp = 12345, .message = "initial message" },
  );
  defer wrapped.deinit(testing.allocator);

  // Manually move the memory to a new buffer (like reading from a file etc.)
  const new_buffer = try testing.allocator.alignedAlloc(u8, @alignOf(@TypeOf(wrapped.memory)), wrapped.memory.len);
  @memcpy(new_buffer, wrapped.memory);
  
  // free the old memory and update the wrapper's memory slice
  testing.allocator.free(wrapped.memory);
  wrapped.memory = new_buffer;

  // internal pointers are now invalid
  wrapped.repointer();

  // Verify that data is correct and pointers are valid
  const entry = wrapped.get();
  try testing.expectEqual(@as(u64, 12345), entry.timestamp);
  try testing.expectEqualSlices(u8, "initial message", entry.message);

  // ensure the slice pointer points inside the *new* buffer
  const memory_start = @intFromPtr(wrapped.memory.ptr);
  const memory_end = memory_start + wrapped.memory.len;
  const slice_start = @intFromPtr(entry.message.ptr);
  const slice_end = slice_start + entry.message.len;
  try testing.expect(slice_start >= memory_start and slice_end <= memory_end);
}


