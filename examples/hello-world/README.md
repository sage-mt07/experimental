# Hello World Example

This sample demonstrates the minimal workflow of **Kafka.Ksql.Linq**.
`Program.cs` contains all logic: it defines a simple POCO entity,
registers it in a context, sends one message with `AddAsync`, waits until the
stream is ready using `WaitForEntityReadyAsync`, and then consumes it with
`ForEachAsync`.

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
