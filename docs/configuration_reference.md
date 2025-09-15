## ⚙️ Kafka.Ksql.Linq appsettings.json Configuration Reference

Kafka.Ksql.Linq uses `appsettings.json` for flexible DSL configuration. This document describes each section. Default values are collected in `examples/configuration/appsettings.json`.

---

### 1 📐 Basic structure

```json
{
  "KsqlDsl": {

    "Common": { /* global Kafka settings */ },
    "Topics": { /* per-topic settings */ },
    "SchemaRegistry": { /* schema registry settings */ },
    "TableCache": [ /* entity cache settings */ ],
    "DlqTopicName": "dead-letter-queue",
    "DlqOptions": { /* DLQ topic settings */ },
    "DeserializationErrorPolicy": "Skip|Retry|DLQ",
    "ReadFromFinalTopicByDefault": false
  }
}
```

---

### 🧱 1.1 Common (Kafka-wide settings)

| Key | Description |
|-----|-------------|
| `BootstrapServers` | Kafka broker endpoints |
| `ClientId` | Client identifier |
| `RequestTimeoutMs` | Operation timeout (ms) |
| `MetadataMaxAgeMs` | Metadata maximum age (ms) |
| `SecurityProtocol` | `Plaintext` / `SaslPlaintext` etc. |
| `SaslMechanism` | e.g. `Plain`, `ScramSha256` |
| `SaslUsername`, `SaslPassword` | SASL credentials |
| `SslCaLocation` | CA certificate path |
| `SslCertificateLocation` | Client certificate path |
| `SslKeyLocation` | Private key path |
| `SslKeyPassword` | Private key password |
| `AdditionalProperties` | Extra Kafka properties (key-value) |

```json
"Common": {
  "BootstrapServers": "localhost:9092",
  "ClientId": "ksql-dsl-client",
  "RequestTimeoutMs": 30000,
  "MetadataMaxAgeMs": 300000,
  "SecurityProtocol": "Plaintext",
  "SaslMechanism": "Plain",
  "SaslUsername": "user",
  "SaslPassword": "pass",
  "SslCaLocation": "/path/ca.pem",
  "SslCertificateLocation": "/path/cert.pem",
  "SslKeyLocation": "/path/key.pem",
  "SslKeyPassword": "secret",
  "AdditionalProperties": {}
}
```

---

### 📦 1.2 Topics (detailed per-topic settings)

Producer settings map to `Kafka.Ksql.Linq.Configuration.Messaging.ProducerSection`, and consumer settings map to `ConsumerSection`. Appsetting keys correspond one-to-one with class properties; extend these classes to add custom options.

```json
"Topics": {
  "my-topic": {
    "Producer": {
      "Acks": "All",
      "CompressionType": "Snappy",
      "EnableIdempotence": true,
      "MaxInFlightRequestsPerConnection": 1,
      "LingerMs": 5,
      "BatchSize": 16384,
      "DeliveryTimeoutMs": 120000,
      "RetryBackoffMs": 100,
      "Retries": 2147483647,
      "BufferMemory": 33554432,
      "Partitioner": null
    },
    "Consumer": {
      "GroupId": "my-group",
      "AutoOffsetReset": "Latest",
      "EnableAutoCommit": true,
      "AutoCommitIntervalMs": 5000,
      "SessionTimeoutMs": 30000,
      "HeartbeatIntervalMs": 3000,
      "MaxPollIntervalMs": 300000,
      "MaxPollRecords": 500,
      "FetchMinBytes": 1,
      "FetchMaxWaitMs": 500,
      "FetchMaxBytes": 52428800,
      "PartitionAssignmentStrategy": null,
      "IsolationLevel": "ReadUncommitted"
    },
    "Creation": {
      "NumPartitions": 1,
      "ReplicationFactor": 1,
      "Configs": {},
      "EnableAutoCreation": false
    }
  }
}
```

| Producer setting | Description |
|------------------|-------------|
| `Acks` | Write acknowledgment strength (`All`, `1`, etc.) |
| `CompressionType` | Compression method (`Snappy`, `Gzip`, `Lz4`, etc.) |
| `EnableIdempotence` | Enables idempotent writes |
| `MaxInFlightRequestsPerConnection` | Max concurrent requests |
| `LingerMs` | Batch wait time (ms) |
| `BatchSize` | Batch size (bytes) |
| `DeliveryTimeoutMs` | Delivery timeout (ms) |
| `RetryBackoffMs` | Retry backoff (ms) |
| `Retries` | Max retry count |
| `BufferMemory` | Buffer size (bytes) |
| `Partitioner` | Partitioner class |
| `AdditionalProperties` | Extra producer settings |

| Consumer setting | Description |
|------------------|-------------|
| `GroupId` | Consumer group ID |
| `AutoOffsetReset` | Offset reset policy |
| `EnableAutoCommit` | Auto commit flag |
| `AutoCommitIntervalMs` | Auto commit interval |
| `SessionTimeoutMs` | Session timeout |
| `HeartbeatIntervalMs` | Heartbeat interval |
| `MaxPollIntervalMs` | Max poll interval |
| `MaxPollRecords` | Max records per poll |
| `FetchMinBytes` | Minimum fetch bytes |
| `FetchMaxWaitMs` | Max fetch wait (ms) |
| `FetchMaxBytes` | Max fetch bytes |
| `PartitionAssignmentStrategy` | Partition assignment strategy |
| `IsolationLevel` | Read isolation level |

#### 🆕 Dynamic topic configuration

Runtime-generated topics (e.g., `rate_1m_pair`, `rate_hb_1m`) inherit `PartitionCount` and `ReplicationFactor` from the `[Topic]` attribute on the base entity. If a complete name exists in `appsettings.json` under `Topics`, those settings override attribute values.

```json
"Topics": {
  "rate_1m": {
    "Creation": {
      "NumPartitions": 2,
      "Configs": { "retention.ms": "60000" }
    }
  },
  "rate_hb_1m": {
    "Creation": {
      "NumPartitions": 3,
      "Configs": { "retention.ms": "120000" }
    }
  }
}
```

In the example, `rate_hb_1m` inherits from `rate_1m` but is overridden by its entry.

---

### 🏪 1.4 Entities (table cache settings)

```json
"Entities": [
  {
    "Entity": "OrderEntity",
    "SourceTopic": "orders",
    "EnableCache": true,
    "StoreName": "orders_store",
    "BaseDirectory": "/var/lib/ksql_cache"
  }
]
```

| Key | Description |
|-----|-------------|
| `Entity` | Target POCO class name |
| `SourceTopic` | Source Kafka topic |
| `EnableCache` | Enable caching (bool) |
| `StoreName` | Cache name (defaults to topic-based name) |
| `BaseDirectory` | Root directory for RocksDB |

---

### 🛡️ 1.5 Validation

- Validation mode is always Strict; the configuration key has been removed.

---

### 💌 1.6 DLQ settings

```json
"DlqTopicName": "dead-letter-queue",
"DlqOptions": {
  "RetentionMs": 5000,
  "NumPartitions": 1,
  "ReplicationFactor": 1,
  "EnableAutoCreation": true,
  "AdditionalConfigs": {
    "cleanup.policy": "delete"
  }
}
```

If unspecified, `DlqTopicName` defaults to `dead-letter-queue`.

| Key | Description |
|------|-------------|
| `DlqTopicName` | DLQ topic name |
| `RetentionMs` | Message retention (ms) |
| `NumPartitions` | Partition count |
| `ReplicationFactor` | Replication factor |
| `EnableAutoCreation` | Automatically create topic |
| `AdditionalConfigs` | Extra topic configs |

---

### 🧩 Mapping DSL to appsettings

| Kafka setting item        | DSL specification                       | appsettings.json key                         | Notes |
|---------------------------|-----------------------------------------|----------------------------------------------|-------|
| Bootstrap Servers         | —                                       | `Kafka:BootstrapServers`                     | Kafka cluster |
| Schema Registry URL       | —                                       | `KsqlDsl:SchemaRegistry:Url`                 | Used when auto-registering POCO schemas |
| ksqlDB URL                | —                                       | `KsqlDsl:KsqlDbUrl`                          | ksqlDB REST endpoint |
| Auto Offset Reset         | `.WithAutoOffsetReset(...)`             | `Kafka:Consumers.<name>.AutoOffsetReset`     | Typically `earliest` or `latest` |
| GroupId                   | `.WithGroupId(...)`                     | `Kafka:Consumers.<name>.GroupId`             | Consumer group ID |
| Topic name                | `[KsqlTopic("orders")]`                 | Overridable via `KsqlDsl:Topics.orders`      | Specify via attribute or Fluent API |
| Partition count           | `[KsqlTopic("orders", PartitionCount=12)]` | `KsqlDsl:Topics.orders.NumPartitions`     | DSL and config both usable |
| Replication factor        | — (set in config)                       | `KsqlDsl:Topics.orders.ReplicationFactor`    | Depends on Kafka cluster |
| DLQ configuration         | `.OnError(ErrorAction.DLQ)`             | `KsqlDsl:DlqTopicName`, `DlqOptions`         | Enable DLQ, retention etc. |

---

### 📦 2. Implementation example (MyKsqlContext & Order & OrderCount)

```csharp
public class Order
{
    public string ProductId { get; set; }
    public decimal Amount { get; set; }
}

public class MyKsqlContext : KsqlContext
{
    protected override void OnModelCreating(KsqlModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>()
            .WithGroupId("orders-consumer")
            .WithAutoOffsetReset(AutoOffsetReset.Earliest);

        modelBuilder.Entity<OrderCount>()
            .WithGroupId("order-counts-consumer")
            .WithAutoOffsetReset(AutoOffsetReset.Latest)
            .UseFinalTopic();
    }
}
```

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Consumers": {
      "orders-consumer": {
        "GroupId": "orders-consumer",
        "AutoOffsetReset": "earliest"
      }
    }
  }
}
```
