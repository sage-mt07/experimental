# Error Handling with DLQ Example

`Program.cs` sends an invalid `Order` to demonstrate `.OnError(ErrorAction.DLQ)`
and `.WithRetry(3)`. Records that fail processing are forwarded to the configured
Dead Letter Queue after retries.

This sample maps to
[advanced_rules.md](../../docs/advanced_rules.md) section about
`OnError` and retry strategies.

## Prerequisites
- .NET 8 SDK
- Docker

## Run Steps
1. Start Kafka and ksqlDB:
   ```bash
   docker-compose up -d
   ```
2. Run the program:
   ```bash
   dotnet run --project .
   ```

Use `LogLevel: Debug` to observe the generated DSL queries in the console.
