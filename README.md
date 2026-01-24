# OneAlloc: Allocate Everything All At Once

Convert complex data structures with nested types, pointers and slices into a **somewhat portable**, **contiguous**; **single** memory allocation.

Merged memory is self-contained. It may be used for:
* On-disk serialization\* (see "Portability" and "Safety" warnings).
* Inter-process communication (IPC).
* Cache-friendly memory layouts.

> [!WARNING]
> **Experimental:** This library is in active development.<br/>
> **Portability:** Same **pointer size** and **endianness** is required for all platforms sharing the data.<br/>
> **Safety:** Only use this on trusted data. It is incredibly easy to make a malicious payload that does out-of-bounds accesses.

# Why OneAlloc?
Structs containing several slices / pointers can lead to poor performance due to scattered memory access patterns, which is bad to CPU caching.
It also makes managing the lifetime of the object simpler.

## Quick Start

```zig
const std = @import("std");
const onealloc = @import("onealloc");

// Define your complex type
const User = struct {
  id: u64,
  name: []const u8,
  roles: []const []const u8,
  metadata: ?*const u32,
};

pub fn main() !void {
  const gpa = std.heap.page_allocator;

  // Create a Wrapper type with default options
  const UserWrapper = onealloc.Wrapper(User, .{});

  var meta: u32 = 42;
  const user_data = User{
    .id = 1234,
    .name = "Zig Programmer",
    .roles = &.{ "admin", "dev" },
    .metadata = &meta,
  };

  // Allocate and merge everything into one block
  var wrapper = try UserWrapper.init(&user_data, gpa);
  defer wrapper.deinit(gpa);

  // Access the data (wrapper.get() returns *User)
  const p = wrapper.get();
  std.debug.print("User: {s}, Role 0: {s}\n", .{ p.name, p.roles[0] });

  // The memory is contiguous!
  std.debug.print("Total allocation: {d} bytes\n", .{wrapper.memory.len});

  // Send memory to other process; use repointer in the other process.
}
```

## Installation

1.  Add to `build.zig.zon`:
  ```zig
  .dependencies = .{
    .onealloc = .{
      .path = "git+https://github.com/SmallThingz/onealloc#<commit>"
      .hash = "<hash>"
    },
  },
  ```

2.  Add to `build.zig`:
  ```zig
  const onealloc_dep = b.dependency("onealloc", .{
    .target = target,
    .optimize = optimize,
  });
  exe.root_module.addImport("onealloc", onealloc_dep.module("onealloc"));
  ```

## Configuration (`MergeOptions`)

See the options struct in `@import("onealloc").MergeOptions`.

**Example:**
```zig
// Don't follow pointers, only handle slices
const MyWrapper = onealloc.Wrapper(MyType, .{ .depointer = false });
```

## Wrappers

There are two types of wrappers available:

### 1. `Wrapper(T, options)`
This is the standard wrapper. It allocates memory for **both** the static structure (`T`) and all dynamic data it points to.
* **Ownership:** The wrapper owns the entire object.
* **Access:** Use `.get()` to access the struct.

### 2. `DynamicWrapper(T, options)`
This wrapper only allocates memory for the **dynamic** parts (indirected children).
* **Ownership:** The wrapper owns the *buffers*, you own the *root struct*.
* **Access:** Access your struct directly.
* **Return:** If `T` is fully static (no pointers), `DynamicWrapper` returns `void`.

```zig
var my_struct = MyStruct{ ... };
var dyn_wrapper = try onealloc.DynamicWrapper(MyStruct, .{}).init(&my_struct, allocator);
defer dyn_wrapper.deinit(allocator);

// my_struct's pointers now point inside dyn_wrapper.memory
```

## API Reference

### `init(val, allocator)` (Static)
Allocates memory and merges the value.
*   `Wrapper`: Takes `*const T`. Returns the wrapper.
*   `DynamicWrapper`: Takes `*T` (mutable). Modifies `val` in-place to point to the new buffer.

### `get()`
*(Wrapper only)* Returns `*T` pointing to the merged data.

### `set(allocator, val)`
Replaces the data in the wrapper.
*   May trigger a reallocation if the new data requires more space.
*   Invalidates previous pointers obtained via `get()`.

### `clone(allocator)`
Deep copies the wrapper and its data into a new allocation.

### `repointer()`
Updates internal pointers to be valid relative to the current `memory` address.
**Crucial for serialization.** See "Serialization & Portability".

> [!WARNING]
> `repointer()` is not safe to use on untrusted data. It is incredibly easy to make a malicious payload that does out-of-bounds accesses.

### `getSize(val)` (Static)
Calculates the number of bytes required to store `val` and all its children.

### `deinit(allocator)`
Frees the memory block owned by the wrapper.

## Serialization & Portability

OneAlloc is designed to make data movable. If you move the memory buffer (e.g., `memcpy`, `write` to disk, `send` over network), the internal pointers will point to the old addresses (invalid).
You will need to use `repointer()` to fix this. Only use this on trusted data.

```zig
// 1. Serialize (write wrapper.memory to disk)
// ...

// 2. Deserialize (read bytes into a buffer)
var raw_buffer = try allocator.alignedAlloc(u8, Wrapper.alignment.toByteUnits(), file_size);
try file.readAll(raw_buffer);

// 3. Load into wrapper
var wrapper = MyWrapper{ .memory = raw_buffer };

// 4. FIX POINTERS
wrapper.repointer();

// 5. Access
const data = wrapper.get();
```

## How It Works: Memory Layout
OneAlloc divides the single memory block into two sections: `[ Static Buffer | Dynamic Buffer ]`
* **Static Buffer:** This part has a fixed size determined at compile-time based on the type `T`. It holds all fixed-size fields (`u32`, `bool`, etc.) and the "headers" for dynamic types (i.e., the `ptr` and `len` of a slice).
* **Dynamic Buffer:** This immediately follows the static buffer. It holds all the variable-sized data that the static part's pointers and slices now point to. For example, the characters of a `[]const u8` are stored here.

## Supported Types
* **Primitives:** `bool`, `int`, `float`, `vector`, `null`, `void`.
* **Pointers:** `*T` (One), `[]T` (Slice).
* **Composites:** `struct`, `array`, `optional`, `error_union`, `union`.
* **Opaques:** Only if they declare `pub const Underlying = onealloc.SerializationFunctions.MergedSignature` and the three required functions (see `onealloc.SerializationFunctions`).

## Limitations

1 **Data Cycles:** A structure that points to itself (directly or indirectly) will cause a stack overflow during merging.
  * *Supported:* Recursive Types (e.g., Linked List definitions).
  * *Unsupported:* Recursive Data (e.g., Node A points to Node B, Node B points to Node A).
2 **Unknown Pointers:** `[*c]`, `[*]`, and `opaque` pointers are compile errors unless `serialize_unknown_pointer_as_usize` is enabled in which case, the literal pointer address is stored.
3 **Data Cycles Will Cause Stack Overflow:** Attempting to merge a data structure with a cycle will cause infinite recursion and crash the program.

