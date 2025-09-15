using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Ddl;
using Kafka.Ksql.Linq.Query.Pipeline;
using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class DDLQueryGeneratorTests
{
    private static T ExecuteInScope<T>(Func<T> func)
    {
        using (ModelCreatingScope.Enter())
        {
            return func();
        }
    }
    private static EntityModel CreateEntityModel()
    {
        return new EntityModel
        {
            EntityType = typeof(TestEntity),
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! },
            AllProperties = typeof(TestEntity).GetProperties()
        };
    }

    [Fact]
    public void GenerateCreateStream_CreatesExpectedStatement()
    {
        var model = CreateEntityModel();
        model.TopicName = "topic";
        model.KeySchemaFullName = "com.acme.Key";
        model.ValueSchemaFullName = "com.acme.Value";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("CREATE STREAM IF NOT EXISTS topic", query);
        Assert.Contains("KAFKA_TOPIC='topic'", query);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='com.acme.Value'", query);
        Assert.Contains("PARTITIONS=1", query);
        Assert.Contains("REPLICAS=1", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateCreateStream_UsesPartitionFromAttribute()
    {
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        var model = builder.GetEntityModel<TestEntity>()!;
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("CREATE STREAM", query);
    }

    [Fact]
    public void GenerateCreateStream_IncludesKeyFormat()
    {
        var model = CreateEntityModel();
        model.KeySchemaFullName = "com.acme.Key";
        model.ValueSchemaFullName = "com.acme.Value";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("KEY_FORMAT='AVRO'", query);
    }

    [Fact]
    public void GenerateCreateTable_IncludesKeyFormat()
    {
        var model = CreateEntityModel();
        model.KeySchemaFullName = "com.acme.Key";
        model.ValueSchemaFullName = "com.acme.Value";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateTable(new EntityModelDdlAdapter(model)));
        Assert.Contains("KEY_FORMAT='AVRO'", query);
        Assert.Contains("PARTITIONS=1", query);
        Assert.Contains("REPLICAS=1", query);
    }

    [Fact]
    public void GenerateCreateTable_AlwaysIncludesValueFormat()
    {
        var model = CreateEntityModel();
        model.ValueSchemaFullName = "com.acme.Value";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateTable(new EntityModelDdlAdapter(model)));
        Assert.Contains("KEY_FORMAT='AVRO'", query);
        Assert.DoesNotContain("KEY_AVRO_SCHEMA_FULL_NAME", query);
        Assert.Contains("VALUE_FORMAT='AVRO'", query);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='com.acme.Value'", query);
    }

    [Fact]
    public void GenerateCreateStream_EmitsKeyFormatWithoutKeySchema()
    {
        var model = CreateEntityModel();
        model.ValueSchemaFullName = "com.acme.Value";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("KEY_FORMAT='AVRO'", query);
        Assert.DoesNotContain("KEY_AVRO_SCHEMA_FULL_NAME", query);
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='com.acme.Value'", query);
    }

    [Fact]
    public void GenerateCreateStream_IncludesSchemaFullNames()
    {
        var model = CreateEntityModel();
        model.KeySchemaFullName = "com.acme.Key";
        model.ValueSchemaFullName = "com.acme.Value";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("VALUE_AVRO_SCHEMA_FULL_NAME='com.acme.Value'", query);
    }

    [Fact]
    public void GenerateCreateStream_WithMultipleKeys_UsesColumnKeys()
    {
        var model = new EntityModel
        {
            EntityType = typeof(MultiKeyEntity),
            TopicName = "multi",
            KeyProperties = new[]
            {
                typeof(MultiKeyEntity).GetProperty(nameof(MultiKeyEntity.Id1))!,
                typeof(MultiKeyEntity).GetProperty(nameof(MultiKeyEntity.Id2))!,
            },
            AllProperties = typeof(MultiKeyEntity).GetProperties()
        };
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("Id1 INT KEY", query);
        Assert.Contains("Id2 INT KEY", query);
        Assert.DoesNotContain("STRUCT", query);
    }

    [Fact]
    public void GenerateCreateStream_SanitizesHyphenName()
    {
        var model = CreateEntityModel();
        model.TopicName = "dead-letter-queue";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
        Assert.Contains("CREATE STREAM IF NOT EXISTS dead_letter_queue", query);
        Assert.Contains("KAFKA_TOPIC='dead-letter-queue'", query);
    }

    [Fact]
    public void GenerateCreateTable_SanitizesHyphenName()
    {
        var model = CreateEntityModel();
        model.TopicName = "dead-letter-queue";
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateTable(new EntityModelDdlAdapter(model)));
        Assert.Contains("CREATE TABLE IF NOT EXISTS dead_letter_queue", query);
        Assert.Contains("KAFKA_TOPIC='dead-letter-queue'", query);
    }

    [Fact]
    public void GenerateCreateTableAs_WithWhereAndGroupBy()
    {
        IQueryable<TestEntity> source = new List<TestEntity>().AsQueryable();
        var expr = source.Where(e => e.IsActive)
                         .GroupBy(e => e.Type)
                         .Select(g => new { g.Key, Count = g.Count() });
        var generator = new DDLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCreateTableAs("t1", "Base", expr.Expression));
        Assert.Contains("CREATE TABLE t1 AS SELECT", query);
        Assert.Contains("FROM Base", query);
        Assert.Contains("WHERE (IsActive = true)", query);
        Assert.Contains("GROUP BY Type", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateCreateStream_OutsideScope_Throws()
    {
        var model = CreateEntityModel();
        var generator = new DDLQueryGenerator();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));

        Assert.Contains("Where/GroupBy/Select", ex.Message);
    }

    private class MultiKeyEntity
    {
        public int Id1 { get; set; }
        public int Id2 { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class WindowEntity
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    [KsqlTopic("attr_entity", PartitionCount = 3, ReplicationFactor = 2)]
    private class AttrEntity
    {
        [KsqlKey]
        public int Id { get; set; }
    }

    [KsqlTopic("attr_view", PartitionCount = 4, ReplicationFactor = 3)]
    private class AttrView
    {
        [KsqlKey]
        public int Id { get; set; }
    }

    private class ConfigView
    {
        [KsqlKey]
        public int Id { get; set; }
    }

    private static string GenerateDdl(EntityModel model)
    {
        var generator = new DDLQueryGenerator();
        return ExecuteInScope(() => generator.GenerateCreateStream(new EntityModelDdlAdapter(model)));
    }

    private static void ApplyTopicConfig(EntityModel model, KsqlDslOptions options)
    {
        if (options.Topics.TryGetValue(model.TopicName!, out var topic) && topic.Creation != null)
        {
            model.Partitions = topic.Creation.NumPartitions;
            model.ReplicationFactor = topic.Creation.ReplicationFactor;
        }
    }

    [Fact]
    public void AttributeEntity_EmitsConfiguredPartitionsAndReplicas()
    {
        var builder = new ModelBuilder();
        builder.Entity<AttrEntity>();
        var model = builder.GetEntityModel<AttrEntity>()!;
        var sql = GenerateDdl(model);
        Assert.Contains("PARTITIONS=3", sql);
        Assert.Contains("REPLICAS=2", sql);
    }

    [Fact]
    public void AttributeToQuery_EmitsConfiguredPartitionsAndReplicas()
    {
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        builder.Entity<AttrView>().ToQuery(q => q.From<TestEntity>().Select(e => new AttrView { Id = e.Id }));
        var model = builder.GetEntityModel<AttrView>()!;
        var sql = GenerateDdl(model);
        Assert.Contains("PARTITIONS=4", sql);
        Assert.Contains("REPLICAS=3", sql);
    }

    [Fact]
    public void ConfigEntity_EmitsConfiguredPartitionsAndReplicas()
    {
        var options = new KsqlDslOptions();
        options.Topics["test-topic"] = new TopicSection
        {
            Creation = new TopicCreationSection { NumPartitions = 5, ReplicationFactor = 2 }
        };
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        var model = builder.GetEntityModel<TestEntity>()!;
        ApplyTopicConfig(model, options);
        var sql = GenerateDdl(model);
        Assert.Contains("PARTITIONS=5", sql);
        Assert.Contains("REPLICAS=2", sql);
    }

    [Fact]
    public void ConfigToQuery_EmitsConfiguredPartitionsAndReplicas()
    {
        var options = new KsqlDslOptions();
        options.Topics["configview"] = new TopicSection
        {
            Creation = new TopicCreationSection { NumPartitions = 6, ReplicationFactor = 1 }
        };
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        builder.Entity<ConfigView>().ToQuery(q => q.From<TestEntity>().Select(e => new ConfigView { Id = e.Id }));
        var model = builder.GetEntityModel<ConfigView>()!;
        ApplyTopicConfig(model, options);
        var sql = GenerateDdl(model);
        Assert.Contains("PARTITIONS=6", sql);
        Assert.Contains("REPLICAS=1", sql);
    }

    [Fact]
    public void DynamicTopic_UsesAttributeConfiguredPartitions()
    {
        var builder = new ModelBuilder();
        builder.Entity<AttrEntity>();
        var model = builder.GetEntityModel<AttrEntity>()!;
        model.TopicName = "attr_entity_hb_1m";
        var sql = GenerateDdl(model);
        Assert.Contains("PARTITIONS=3", sql);
        Assert.Contains("REPLICAS=2", sql);
    }

    [Fact]
    public void DynamicTopic_UsesAppsettingsConfiguredPartitions()
    {
        var options = new KsqlDslOptions();
        options.Topics["base_hb_1m"] = new TopicSection
        {
            Creation = new TopicCreationSection { NumPartitions = 5, ReplicationFactor = 2 }
        };
        var builder = new ModelBuilder();
        builder.Entity<TestEntity>();
        var model = builder.GetEntityModel<TestEntity>()!;
        model.TopicName = "base_hb_1m";
        ApplyTopicConfig(model, options);
        var sql = GenerateDdl(model);
        Assert.Contains("PARTITIONS=5", sql);
        Assert.Contains("REPLICAS=2", sql);
    }

    [Fact]
    public void GenerateCreateTableAs_MultipleWindowStart_Throws()
    {
        IQueryable<WindowEntity> src = new List<WindowEntity>().AsQueryable();

        var expr = src
            .Tumbling(e => e.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(e => e.Id)
            .Select(g => new { Start1 = g.WindowStart(), Start2 = g.WindowStart() });

        var generator = new DDLQueryGenerator();
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ExecuteInScope(() => generator.GenerateCreateTableAs("t1", "src", expr.Expression)));
        Assert.Contains("Windowed query requires exactly one WindowStart() in projection.", ex.Message);
    }
}
