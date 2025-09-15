using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Ddl;

public class DdlSchemaBuilder
{
    private readonly string _objectName;
    private readonly DdlObjectType _objectType;
    private readonly string _topicName;
    private readonly int _partitions;
    private short _replicas;
    private readonly List<ColumnDefinition> _columns = new();
    private string? _keySchemaFullName;
    private string? _valueSchemaFullName;
    private string? _timestampColumn;

    public DdlSchemaBuilder(string objectName, DdlObjectType objectType, string topicName, int partitions = 1, short replicas = 1)
    {
        _objectName = objectName;
        _objectType = objectType;
        _topicName = topicName;
        _partitions = partitions;
        _replicas = replicas;
    }

    public DdlSchemaBuilder WithReplicas(short replicas)
    {
        _replicas = replicas;
        return this;
    }

    public DdlSchemaBuilder AddColumn(string name, string type, bool isKey = false)
    {
        _columns.Add(new ColumnDefinition(name, type, isKey));
        return this;
    }

    public DdlSchemaBuilder WithSchemaFullNames(string? keySchemaFullName, string? valueSchemaFullName)
    {
        _keySchemaFullName = keySchemaFullName;
        _valueSchemaFullName = valueSchemaFullName;
        return this;
    }

    public DdlSchemaBuilder WithTimestamp(string? columnName)
    {
        _timestampColumn = columnName;
        return this;
    }

    public DdlSchemaDefinition Build()
    {
        return new DdlSchemaDefinition(
            _objectName,
            _topicName,
            _objectType,
            _partitions,
            _replicas,
            _keySchemaFullName,
            _valueSchemaFullName,
            _columns,
            _timestampColumn);
    }
}
