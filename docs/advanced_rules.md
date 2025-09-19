# Advanced Rules and Patterns (Bars, Schedules, Weekly Handling)

This guide explains key points for real-world operations such as event-time based bar generation, MarketSchedule-driven aggregation, weekly handling, and late record management. It highlights differences between the DSL and ksqlDB to reduce misunderstandings.

## 1. Event Time is the reference
- Bars (tumbling windows) are rounded and aggregated by the event time of the Kafka record.
- In tests and batches you can set `Rate.Timestamp` explicitly, allowing immediate creation of windows in the past or future without waiting for real time.
- Operational waiting is limited to service initialization, DDL stabilization, and polling for query responses; it does not depend on the window length.

## 2. Windows and EMIT (CHANGES / FINAL)
- Aggregations use tumbling windows (e.g., 1m/5m/15m/60m or 1d/7d).
- Use `EMIT CHANGES` for live flows and `EMIT FINAL` for finalized results.
- Define Close with `LatestByOffset(...)` and combine with Open/High/Low to build OHLC.

## 3. Week anchor and ksqlDB behavior
- The DSL has a WeekAnchor concept with Monday as the default start of week.
- ksqlDB windows themselves have no weekday anchor; `SIZE 7 DAYS` cuts at epoch-based intervals.
- In practice, use a **MarketSchedule** (business calendar) to decide the **MarketDate** and join with a **TimeFrame** to key by day (`dayKey`). This guarantees logical week starts and holiday handling.

### Recommended pattern for Monday-based weeks
1. Insert `Broker, Symbol, MarketDate (working day), Open, Close` into MarketSchedule (omit weekends).
2. Use `TimeFrame<MarketSchedule>` in the DSL and join where `s.Open <= r.Timestamp && r.Timestamp < s.Close`.
3. Specify `dayKey: s => s.MarketDate` to stabilize daily/weekly keys based on MarketSchedule.
4. Aggregate weekly with Tumbling Days=7; assuming MarketSchedule only supplies weekdays.

## 4. Modeling and using MarketSchedule
- Topic example: `marketschedule`
- Suggested fields:
  - `Broker, Symbol` (key)
  - `Open, Close` (business start/end)
  - `MarketDate` (representative date; expresses week start and holidays)
- DSL example (pseudo code):

```csharp
modelBuilder.Entity<Bar>()
  .ToQuery(q => q.From<Rate>()
    .TimeFrame<MarketSchedule>((r, s) =>
         r.Broker == s.Broker
      && r.Symbol == s.Symbol
      && s.Open <= r.Timestamp && r.Timestamp < s.Close,
      dayKey: s => s.MarketDate)
    .Tumbling(r => r.Timestamp, new Windows { Days = new[] { 1, 7 } })
    .GroupBy(r => new { r.Broker, r.Symbol })
    .Select(g => new Bar
    {
        Broker = g.Key.Broker,
        Symbol = g.Key.Symbol,
        BucketStart = g.WindowStart(),
        Open  = g.EarliestByOffset(x => x.Bid),
        High  = g.Max(x => x.Bid),
        Low   = g.Min(x => x.Bid),
        Close = g.LatestByOffset(x => x.Bid)
    }));
```

## 5. Handling non-business days (e.g., weekends)
- Supply MarketSchedule **only for business days**; exclude weekends so daily bars are not generated for those dates.
- Weekly aggregation only covers weekdays, so a 7-day tumbling window effectively counts business days.
- Physical test example:
  - Insert schedule records for the latest Monday–Friday (no weekend rows).
  - Insert one `Rate` record at noon each day from Monday through Sunday.
  - Expect `bar_1d_live` to contain five weekday rows and `bar_1wk_final` to contain one weekly row.

## 6. Late records (grace) and boundary handling
- Setting `grace` (allowed lateness) lets events arriving after the boundary still update the window if their event time falls inside it, updating High/Low/Close.
- Tests inject extreme values just before and after boundaries to ensure no misrouting between adjacent windows.

## 7. Creating multiple tiers (1m/5m/15m/60m/1d/1wk)
- Use `new Windows { Minutes = new[] { 1, 5, 15, 60 }, Days = new[] { 1, 7 } }` to specify multiple frames simultaneously.
- DSL → QueryModel → DDL generates derived CSAS/CTAS for each tier: `bar_1m_live`, `bar_5m_live`, `bar_15m_live`, `bar_60m_live`, `bar_1d_live`, `bar_1wk_final`.
- Derived stages use `SELECT *`, so the column set is based on the application; no fixed column names.

## 8. Push vs Pull (recommended HTTP calls)
- Push: `SELECT ... EMIT CHANGES LIMIT N` (/query-stream) to wait for generation and avoid missing messages.
- Pull: `SELECT ... FROM <table>` (/query) to obtain and verify finalized state.
- For robustness, send `/query` requests as `{"sql":"...","properties":{}}`, with optional `ksql` field or fallback to push when necessary.

## 9. Operational tips (performance/stability)
- Logs: `KSQL_LOG4J_ROOT_LOGLEVEL=INFO` (suppress DEBUG)
- GC: `-XX:+UseG1GC -XX:MaxGCPauseMillis=100` (favor short pauses)
- Queries: adjust `KSQL_KSQL_QUERY_TIMEOUT_MS=300000` (5 min) and `KSQL_KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG`
- Internal/external Kafka listeners: set `PLAINTEXT://localhost:9092, INTERNAL://kafka:29092` properly; ksqlDB/Schema Registry use `kafka:29092`

## 9.5 Persistent Query Stabilization
- EnsureQueryEntityDdlAsync logs queryId, polls internal topics, waits for consumer group stabilization.
- Retries with TERMINATE→rerun until timeout (default 45s) or max attempts (default 3).
- Controlled by KSQL_PERSISTENT_QUERY_READY_TIMEOUT_SECONDS / MAX_ATTEMPTS.
- Skips stabilization if no Kafka admin access, logs warning.

## 10. Key patterns (summary)
- Week concept: DSL's WeekAnchor is Monday; since ksqlDB windows lack weekday anchors, pin it logically with MarketSchedule `MarketDate`.
- Daily/weekly: `TimeFrame + Tumbling(Days={1|7})`, use `g.WindowStart()` as `BucketStart`, and build OHLC with Earliest/Max/Min/Latest.
- Non-business days: supply MarketSchedule only for business days; daily bars appear only on weekdays.
- Late/boundary: allow grace to absorb event-time-based updates; test extreme values around boundaries.

---
Content is physically verified in `physicalTests` for long runs, multi-tier setups, and schedule dependencies. When using your own schedules or holiday calendars, design MarketSchedule (`MarketDate/Open/Close`) appropriately and build DSL on these patterns.

## Appendix: Sample data for weekly MarketSchedule

Example for Monday-based weeks with weekend closure. `Broker/Symbol` fixed (B1/S1) and UTC business hours 09:00–15:00. Do not insert rows for weekends.

### 1) Conceptual records (Monday–Friday)

| Broker | Symbol | MarketDate (UTC) | Open (UTC)        | Close (UTC)       |
|--------|--------|------------------|-------------------|-------------------|
| B1     | S1     | 2025-09-08       | 2025-09-08 09:00  | 2025-09-08 15:00  |
| B1     | S1     | 2025-09-09       | 2025-09-09 09:00  | 2025-09-09 15:00  |
| B1     | S1     | 2025-09-10       | 2025-09-10 09:00  | 2025-09-10 15:00  |
| B1     | S1     | 2025-09-11       | 2025-09-11 09:00  | 2025-09-11 15:00  |
| B1     | S1     | 2025-09-12       | 2025-09-12 09:00  | 2025-09-12 15:00  |

> Note: do not insert rows for Saturday 2025-09-13 or Sunday 2025-09-14.

### 2) ksqlDB ingestion (for pull/push verification)

DDL (usually auto-generated):

```sql
CREATE STREAM IF NOT EXISTS MARKETSCHEDULE (
  BROKER STRING KEY,
  SYMBOL STRING KEY,
  OPEN   TIMESTAMP,
  CLOSE  TIMESTAMP,
  MARKETDATE TIMESTAMP
) WITH (
  KAFKA_TOPIC='marketschedule',
  KEY_FORMAT='AVRO', VALUE_FORMAT='AVRO'
);
```
