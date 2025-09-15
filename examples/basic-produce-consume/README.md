# Basic Produce and Consume Example

This sample pairs a simple producer and consumer using **Kafka.Ksql.Linq**.
`Program.cs` registers `BasicMessage` with `[Topic]` and demonstrates
sending a record then retrieving it with `ForEachAsync`.

For an inline walkthrough, see the minimal example in the project root `README.md`,
section *3. POCO attribute-based DSL design rules*.

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

Query details are visible in debug logs when `LogLevel: Debug` is enabled
in `appsettings.json`.
