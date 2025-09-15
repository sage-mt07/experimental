# Windowing (unified)

Purpose: gather the basics of TUMBLING, HOPPING, and SESSION windows alongside:
- Live aggregation (Push, EMIT CHANGES)
- 1-minute → 5-minute roll-ups
so ordinary developers can see everything in one place.

Unified from
- former `examples/tumbling-live-consumer`
- former `examples/rollup-1m-5m-verify`

Prerequisites
- .NET 8 / Docker
- Start Kafka + Schema Registry + ksqlDB (`docker-compose -f tools/docker-compose.kafka.yml up -d`)

Minimal steps
1) Define windows and aggregates in OnModelCreating (see `../../docs/onmodelcreating_samples.md#7-time-window-tumbling-1min-push`).
2) Feed sample data (any Producer is fine, `examples/basic-produce-consume` works).
3) Use a Push query (EMIT CHANGES) to watch live results.
4) Layer a 5-minute roll-up on top of the 1-minute aggregation and confirm the numbers align.

Notes
- For diagrams of Streams/Tables and Pull/Push, see `../../docs/sqlserver-to-kafka-guide.md`.
- ksqlDB function/type mapping: `../../docs/ksql-function-type-mapping.md`
