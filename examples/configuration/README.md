# Configuration (sample)

Purpose: grasp the minimal setup for appsettings.json and Builder configuration.

## What this covers
- Connection settings for Kafka/Schema Registry/ksqlDB
- Basic properties for Topic/Consumer/Producer
- Applying options via DSL/Builder

## Prerequisites
- .NET 8 SDK
- Local Kafka/Schema Registry/ksqlDB (`docker-compose -f tools/docker-compose.kafka.yml up -d`)

## Run
```
cd examples/configuration
# edit appsettings.json if needed
# follow project instructions such as `dotnet run`
```

## Related samples
- `examples/configuration-mapping`: extended mapping settings
- `examples/schema-attributes`: `[KsqlKey]` / `[KsqlDecimal]` / `[KsqlTimestamp]`

## References
- Function/type mapping: `../../docs/ksql-function-type-mapping.md`
- SQLServer → ksqlDB guide: `../../docs/sqlserver-to-kafka-guide.md`
