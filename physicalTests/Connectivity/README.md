# Connectivity Tests

These tests validate that each service in the docker environment is reachable.
Run them sequentially so any failure clearly indicates which component is
unavailable.

## Recommended order and verification points

1. **PortConnectivityTests** – confirm each service port responds
   - `Kafka_Broker_Should_Be_Reachable` &mdash; connect via Kafka Admin client
   - `SchemaRegistry_Should_Be_Reachable` &mdash; HTTP GET to Schema Registry
   - `KsqlDb_Should_Be_Reachable` &mdash; execute a simple statement against ksqlDB

2. **KafkaConnectivityTests** – ensure producer/consumer round trip works and ksqlDB metadata is accessible

3. **SchemaRegistryResetTests** – verifies schema registration handling without relying on a shared reset helper

4. **KafkaServiceDownTests** – verify operations throw when Kafka is stopped

5. **KsqlDbServiceDownTests** – verify statements fail when ksqlDB is stopped

6. **BigBang_KafkaConnection_TolerantTests** – operations immediately fail with clear errors while Kafka is down

7. **BigBang_KafkaConnection_StrictTests** – operations keep retrying until timeout when Kafka is down

The individual port checks are defined in `PortConnectivityTests.cs`. A basic
producer/consumer round trip is provided in `KafkaConnectivityTests.cs` once all ports respond.
