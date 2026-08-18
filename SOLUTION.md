# SOLUTION.md

## Incident

The service accepted call-completion webhooks from a provider that delivers
at-least-once, and maintained per-account call statistics. Operations reported
duplicate call records, drifting call-counts, recordings never marked
processed with nothing in the logs, and in-flight work disappearing on every
deploy.

## What was broken, and why

### 1. Recording processing cancelled by request context

- **Root cause:** `Ingest` spawned a goroutine and passed it the HTTP request
  context. net/http cancels a request's context when `ServeHTTP` returns, and
  the handler returns immediately. By the time `processRecording` finished its
  simulated 50 ms download and called `MarkRecordingProcessed`, the context was
  cancelled and the UPDATE failed with `context canceled`.
- **Silence:** the goroutine's error went to a `// TODO: handle` comment —
  nothing was logged, matching "nothing in the logs about it".
- **Fix:** the goroutine now uses `context.Background()`, and failures are
  logged with slog (`recording processing failed`, with `call_id` and error).
- **Test:** `TestRecordingGetsMarkedProcessed` posts a webhook and polls
  `calls.recording_processed`. It failed before the fix
  ("recording was never marked processed") and passes after.

### 2. Stats cache race

- **Root cause:** `Cache.Get` took the read lock but `Cache.Record` took no
  lock at all. Concurrent webhooks raced on the map (read and assignment) and
  on the `CallCount`/`TotalDurationSec` read-modify-write updates, losing
  increments and occasionally overwriting fresh account entries.
- **Fix:** `Record` now holds the same `sync.RWMutex` (`Lock`/`defer
  Unlock`) that `Get` already used.
- **Test:** `TestCacheRecordIsAccurateUnderConcurrency` runs 16 goroutines ×
  2,000 records against one account and asserts exactly 32,000/32,000.
  Before the fix, `go test -race` reported data races on `cache.go:40,43,45,46`
  and the count came out 19,650/24,250 (lost updates). After the fix the count
  is exact and the race detector is clean.

### 3. Webhook idempotency race (duplicates + double counting)

- **Root cause:** `Ingest` did a check-then-insert — `EventExists` followed by
  `InsertEvent` — and `events.event_id` had only a non-unique index. Two
  concurrent redeliveries of the same `event_id` both passed the check and both
  inserted, and each one then incremented `account_stats` and the cache.
  Side effects ran in three separate transactions, so they could never be
  atomic with the dedup decision. A partial failure stored the event but
  skipped call/stats, and the provider's retry was then dropped because
  `EventExists` returned true.
- **Fix (deduplication strategy):** Postgres is the source of truth.
  Migration `002_events_unique_event_id.sql` removes any pre-fix duplicates
  and adds a unique index on `events.event_id`. `Store.IngestEvent` then
  performs everything in one transaction, with the INSERT itself as the gate:
  `INSERT INTO events ... ON CONFLICT (event_id) DO NOTHING`. Zero rows
  affected means the event was already ingested — nothing is written and
  nothing is counted. Call upsert and the stats increment run in the same
  transaction, so a failure rolls back everything and a retry re-attempts
  cleanly.
- **Why Postgres over the alternatives:** the dedup decision must be atomic
  with the rows it protects, and it must survive restarts. A Redis `SETNX`
  gate cannot be atomic with the Postgres writes and its keys can be lost
  (eviction, restart). A Postgres unique constraint gives correctness with no
  extra moving parts. Redis remains available for fast-path filtering or
  queueing, but neither is required for correctness.
- **Test:** `TestConcurrentDuplicateDeliveryCountsOnce` fires 20 concurrent
  POSTs of the same `event_id` and asserts exactly one `events` row, one
  `calls` row, and `call_count = 1`. Before the fix it failed with
  `events=6 stats=6` (6 duplicates double-counted); after the fix it passes,
  including across repeated runs.

### 4. State lost on restart/deploy

- **Root cause:** two pieces of state were process-local. The in-memory stats
  cache was created empty on every boot and `GET /accounts/{id}/stats` serves
  only the cache, so a deploy reset the dashboard numbers to zero even though
  the durable `account_stats` table was intact. Recording work ran in bare
  goroutines with no durable marker of being in progress; a deploy killed them
  mid-flight, the `recording_processed` UPDATE never landed, and nothing ever
  retried it.
- **Fix:** recover both from durable state at startup. `Store.AllAccountStats`
  + `Cache.Put` rebuild the cache (`HydrateFromStore`).
  `Store.PendingRecordings` finds calls with
  `recording_url IS NOT NULL AND recording_processed = FALSE`, and
  `ReplayPendingRecordings` re-runs processing for them (the marker update is
  idempotent, so replay may overlap live processing safely). `Service.Restore`
  runs both, and `cmd/server/main.go` calls it before the server starts — if
  `Restore` fails, main logs and exits (fail fast), matching the existing
  Postgres/Redis connection handling. The test harness (`testutil.NewService`)
  wires services exactly like production so tests exercise the same path.
- **Tests:** `TestStatsSurviveRestart` ingests into one server, closes it
  ("deploy"), starts a fresh server against the same database and expects the
  stats endpoint to return the durable 1/143 — it returned 0/0 before the
  fix. `TestPendingRecordingIsReplayedAfterRestart` seeds an unprocessed call
  row, starts a fresh service, and expects the recording to be marked
  processed — it was never picked up before the fix.

## Overall root cause

Correctness depended on short-lived, process-local state: request contexts,
in-memory maps, and fire-and-forget goroutines. Anything that outlived a
request or a process — context cancellation, a deploy, concurrent deliveries —
could silently corrupt or drop results. The fixes moved the decisions and the
recovery onto the durable layer (unique `event_id`, one transaction per
ingest, `recording_processed` as a resumable marker) and made the in-memory
state a restorable view of the database instead of an independent source of
truth.

## Tests and verification

The verification commands were run against the Compose stack after all four
fixes were implemented:

- `go test ./...` — all packages pass
- `go test -race ./...` — all packages pass, race detector clean
- `go vet ./...` — clean
- `git diff --check` — clean (no whitespace errors)

The original test suite did not exercise these paths concurrently or across
process restarts; each defect now has a fail-first test that reproduces the
incident and passes after its fix.

## Commit history

- `e858381` fix: process recordings with a detached context and log —
  Bug 1 (request-context cancellation, silent failures)
- `89bc9e1` fix: guard stats cache Record with the mutex to prevent lost
  updates — Bug 2 (cache data race)
- `3bf1633` fix: dedupe webhooks atomically via unique event_id in one
  transaction — Bug 3 (idempotency)
- `d23b2c9` fix: restore cache and resume pending recordings at startup, fail
  fast on error — Bug 4 (state loss on deploy)

Each change is a single logical fix with its own fail-first test, committed
independently. The working tree is clean and `main` is up to date with
`origin/main`.

## Future scaling considerations

- **Ingest path:** batch the DB writes (multi-row inserts or `COPY`), or ship
  events to Redis Streams with consumer groups and a fixed worker pool,
  instead of one goroutine and one transaction round-trip per webhook.
  Pre-filter duplicates with a Redis `SETNX` fast path in front of the
  Postgres gate (correctness still lives in the unique constraint).
- **Recording processing:** replace per-webhook goroutines with a durable
  queue (outbox table or Redis Stream) and a bounded worker pool with
  retries/backoff; the startup replay stays as the safety net.
- **Stats:** keep the cache but make it write-behind with periodic reconcile
  against `account_stats`, shard by account, and scale the reads off read
  replicas.
- **Schema/ops:** partition `events` by time, right-size
  `DB_MAX_CONNS`, and run several app replicas behind a load balancer — the
  idempotent, transactional ingest already makes horizontal scaling safe.
