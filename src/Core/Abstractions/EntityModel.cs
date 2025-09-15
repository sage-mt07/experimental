using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Kafka.Ksql.Linq.Core.Abstractions;

public class EntityModel
{
    public Type EntityType { get; set; } = null!;

    public string? TopicName { get; set; }

    /// <summary>
    /// LINQ expression used for query-based entity definitions.
    /// </summary>
    public LambdaExpression? QueryExpression { get; set; }

    /// <summary>
    /// New DSL query model for CREATE statements.
    /// </summary>
    public Kafka.Ksql.Linq.Query.Dsl.KsqlQueryModel? QueryModel { get; set; }

    /// <summary>
    /// Number of partitions for the backing Kafka topic.
    /// </summary>
    public int Partitions { get; set; } = 1;

    /// <summary>
    /// Replication factor for the backing Kafka topic.
    /// </summary>
    public short ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Fully-qualified Avro schema name for the key.
    /// </summary>
    public string? KeySchemaFullName { get; set; }

    /// <summary>
    /// Fully-qualified Avro schema name for the value.
    /// </summary>
    public string? ValueSchemaFullName { get; set; }

    public PropertyInfo[] KeyProperties { get; set; } = Array.Empty<PropertyInfo>();

    public PropertyInfo[] AllProperties { get; set; } = Array.Empty<PropertyInfo>();

    /// <summary>
    /// Optional selector expression identifying the bar timestamp used for
    /// ordering or limiting operations. This is automatically populated when
    /// <c>Select&lt;TResult&gt;()</c> is used with Window DSL and a property assignment
    /// from <c>WindowGrouping.BarStart</c> is detected.
    /// </summary>
    public LambdaExpression? BarTimeSelector { get; set; }

    /// <summary>
    /// Indicates whether this entity is used for reading, writing, or both.
    /// </summary>
    public EntityAccessMode AccessMode { get; set; } = EntityAccessMode.ReadWrite;

    public ValidationResult? ValidationResult { get; set; }

    public bool IsValid => ValidationResult?.IsValid ?? false;
    public StreamTableType StreamTableType
    {
        get
        {
            if (_explicitStreamTableType.HasValue)
                return _explicitStreamTableType.Value;

            return StreamTableType.Stream;
        }
    }
    /// <summary>
    /// Explicitly set the Stream/Table classification.
    /// </summary>
    /// <param name="streamTableType">The classification to set.</param>
    public void SetStreamTableType(StreamTableType streamTableType)
    {
        _explicitStreamTableType = streamTableType;
    }

    private StreamTableType? _explicitStreamTableType;

    /// <summary>
    /// Returns the explicitly set Stream/Table classification if present.
    /// </summary>
    public StreamTableType GetExplicitStreamTableType()
    {
        return _explicitStreamTableType ?? StreamTableType;
    }

    /// <summary>
    /// Checks whether key properties exist.
    /// Rationale: needed for Stream/Table determination (same as CoreExtensions.HasKeys()).
    /// </summary>
    public bool HasKeys()
    {
        return KeyProperties != null && KeyProperties.Length > 0;
    }

    /// <summary>
    /// Checks whether the key is composite.
    /// </summary>
    public bool IsCompositeKey()
    {
        return KeyProperties != null && KeyProperties.Length > 1;
    }
    /// <summary>
    /// Action to take when a processing error occurs.
    /// </summary>
    public ErrorAction ErrorAction { get; set; } = ErrorAction.Skip;

    /// <summary>
    /// Policy to apply on deserialization failure.
    /// </summary>
    public DeserializationErrorPolicy DeserializationErrorPolicy { get; set; } = DeserializationErrorPolicy.Skip;

    /// <summary>
    /// Enables RocksDB cache usage.
    /// </summary>
    public bool EnableCache { get; set; } = true;

    /// <summary>
    /// Optional consumer group id specified via DSL or configuration.
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Additional extensible settings. Designed for future DSL/config items.
    /// </summary>
    public Dictionary<string, object> AdditionalSettings { get; } = new();

}
