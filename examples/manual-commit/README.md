# Manual Commit Example

This sample demonstrates manual acknowledgement using **Kafka.Ksql.Linq**.
Manual commit is selected at runtime by passing `autoCommit: false` to `ForEachAsync`.
During consumption each record is passed to the delegate as the POCO instance.
Call `context.Orders.Commit(entity)` after successful processing to record the offset.

## Prerequisites

- .NET 8 SDK
- Docker (for Kafka and ksqlDB)

## Setup

1. Start the local Kafka stack:
   ```bash
   docker-compose up -d
   ```
2. Run the example:
   ```bash
   dotnet run --project .
   ```

## Design Document References

- [Kafka.Ksql.Linq user guide](../../docs/kafka_ksql_linq_user_guide.md)
- [API reference](../../docs/api_reference.md)
