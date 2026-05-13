# PRD: V2 projection engine support on `create_projection()`

## 1. Background

KurrentDB is rolled out a new projection runtime, **V2 engine**. It is opt-in
per projection and must be selected **at create time** — the engine version
is pinned for the lifetime of the projection and cannot be migrated by an
update. V1 remains the default to keep all existing projections behaving
exactly as before.

The wire-level switch is a single new field on
`projectionmanagement.proto :: CreateReq.Options`:

```proto
message CreateReq {
  Options options = 1;
  message Options {
    oneof mode {
      event_store.client.Empty one_time = 1;
      Transient transient = 2 [deprecated = true];
      Continuous continuous = 3;
    }
    string query = 4;
    int32  engine_version = 5;   // NEW. 0 or 1 = v1 (default), 2 = v2
    // ...
  }
}
```

Implementation rules:

- **Default is V1.** The wire field is only set when the caller explicitly
  asks for V2. This keeps the "old client + new server + omitted field"
  matrix safe (`0` and `1` both mean V1).
- **The public enum is decoupled from the wire encoding.** Expose string
  values (`"v1"` / `"v2"`) and convert to the `int32` at the gRPC boundary.
  This makes the API stable if the wire layout ever shifts to a proper proto
  enum.
- **V2 has documented limitations.** V2 does **not** support
  `track_emitted_streams`, bi-state projections, or live `outputState`
  result streams. The server documentation is the source of truth; the
  Python client should reproduce the relevant warnings inline so users hit
  them at API surface time, not at runtime.

The server also exposes the same selector over HTTP as the
`engineversion=2` query param. The Python client has **no HTTP fallback**
for projections, so this PRD only addresses the gRPC path.
