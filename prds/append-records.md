# PRD: `append_records()` — atomic multi-stream append with cross-stream consistency checks

## 1. Background

KurrentDB 26.1 added a new RPC, **`AppendRecords`**, on the `streams.v2`
service. It complements the existing `AppendSession` RPC (already exposed
in the client as `multi_append_to_stream()`) with two semantic differences:

1. **Records are interleaved across streams in a single request.** The
   exact send order is preserved in the global log, so callers can assert
   "event A on stream X happens-before event B on stream Y" inside a
   single atomic transaction.
2. **Consistency checks are decoupled from writes.** A check can reference
   *any* stream — including streams the caller is not writing to. This
   enables **Dynamic Consistency Boundary (DCB)** patterns where a
   business decision depends on the state of multiple streams but the
   resulting events are written to a different (and possibly smaller)
   subset of streams.

This PRD describes the feature the client should expose. It does not
prescribe internals.

---

## 2. Feature surface

The client should expose a single new method:

```
append_records(records, checks=None) -> AppendRecordsResult
```

available on both the sync and async client, as a peer of the existing
`append_to_stream()` and `multi_append_to_stream()`.

**Inputs**

- `records` — one or more records to append. Each record carries a target
  stream name and an event payload (same event shape used by
  `multi_append_to_stream()`: type, data, content_type, optional metadata,
  optional id).
- `checks` (optional) — zero or more consistency checks evaluated atomically
  before commit. Each check is a *stream state check*: it asserts that a
  named stream is at a specific state (`ANY`, `NO_STREAM`, `EXISTS`, or a
  specific revision). The stream named in a check does **not** have to be
  one of the streams written to.

**Output**

- A result object that exposes:
  - the **global commit position** of the transaction;
  - per-stream **resulting revisions** (one entry per distinct stream that
    received records).

**Atomicity guarantees** (from the server contract):

- All records succeed or all fail together.
- The global log preserves the exact order of `records` in the request.
- All checks are evaluated atomically before commit; if any check fails,
  no records are written.

**Metadata**

Match the constraint already enforced by `multi_append_to_stream()`:
metadata, when provided, must be a JSON object whose values are strings.
Anything else is rejected with the same error type used by
`multi_append_to_stream()`.

---

## 3. Server contract

The full server contract for this feature is defined in the KurrentDB
proto files. The relevant sections:

### `streams.proto`

- **`AppendRecords` RPC** (signature, guarantees, DCB rationale):
  https://github.com/kurrent-io/KurrentDB/blob/c6d39a9bf6f84accd3ec59ded8cfc72fc5a3b991/proto/kurrentdb/protocol/v2/streams/streams.proto#L37-L57

- **`AppendRecord` message** — note that `stream` is required for
  `AppendRecords` (and ignored for `AppendSession`):
  https://github.com/kurrent-io/KurrentDB/blob/c6d39a9bf6f84accd3ec59ded8cfc72fc5a3b991/proto/kurrentdb/protocol/v2/streams/streams.proto#L127-L131

- **`AppendRecordsRequest`, `AppendRecordsResponse`, `ConsistencyCheck`,
  `StreamRevision`** — request/response shapes and the
  `expected_state` encoding (`-1=NO_STREAM`, `-2=ANY`, `-4=EXISTS`,
  `n>=0=revision`):
  https://github.com/kurrent-io/KurrentDB/blob/c6d39a9bf6f84accd3ec59ded8cfc72fc5a3b991/proto/kurrentdb/protocol/v2/streams/streams.proto#L154-L233

### `errors.proto`

- **`STREAMS_ERROR_APPEND_CONSISTENCY_VIOLATION`** error code (raised
  when one or more checks fail; the entire transaction is aborted):
  https://github.com/kurrent-io/KurrentDB/blob/c6d39a9bf6f84accd3ec59ded8cfc72fc5a3b991/proto/kurrentdb/protocol/v2/streams/errors.proto#L148-L168

- **`AppendConsistencyViolationErrorDetails` / `ConsistencyViolation` /
  `StreamStateViolation`** — structured details payload describing every
  failed check, including its index in the original list, the stream
  name, the expected state, and the actual state observed
  (`n>=0`=revision, `-1`=no stream, `-5`=deleted, `-6`=tombstoned):
  https://github.com/kurrent-io/KurrentDB/blob/c6d39a9bf6f84accd3ec59ded8cfc72fc5a3b991/proto/kurrentdb/protocol/v2/streams/errors.proto#L235-L283

When the server returns `STREAMS_ERROR_APPEND_CONSISTENCY_VIOLATION`, the
client should surface a typed exception that carries the structured list
of violations (one entry per failed check, with `check_index`,
`stream_name`, `expected_state`, `actual_state`) so the caller can react
without parsing strings.

---

## 4. Server version

`AppendRecords` is available from **KurrentDB 26.1**.

Calling it against an older server returns gRPC `UNIMPLEMENTED`; the
client should let that surface as the existing unsupported-feature error
type already used by `multi_append_to_stream()`.
