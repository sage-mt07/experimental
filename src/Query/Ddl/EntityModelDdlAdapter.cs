using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Core.Models;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Ddl;

public class EntityModelDdlAdapter : IDdlSchemaProvider
{
    private readonly EntityModel _model;

    public EntityModelDdlAdapter(EntityModel model)
    {
        _model = model;
    }

    public DdlSchemaDefinition GetSchema()
    {
        var builder = new DdlSchemaBuilder(
            _model.EntityType.Name.ToLowerInvariant(),
            _model.GetExplicitStreamTableType() == StreamTableType.Table ? DdlObjectType.Table : DdlObjectType.Stream,
            _model.GetTopicName(),
            _model.Partitions > 0 ? _model.Partitions : 1,
            _model.ReplicationFactor > 0 ? _model.ReplicationFactor : (short)1)
            .WithSchemaFullNames(_model.KeySchemaFullName, _model.ValueSchemaFullName);
        // Detect timestamp column from model attributes
        string? tsColumn = null;
        try
        {
            var tsProp = _model.AllProperties.FirstOrDefault(p => p.GetCustomAttributes(true).OfType<Kafka.Ksql.Linq.Core.Attributes.KsqlTimestampAttribute>().Any());
            if (tsProp != null)
                tsColumn = KsqlNameUtils.Sanitize(tsProp.Name);
        }
        catch { }
        if (!string.IsNullOrWhiteSpace(tsColumn))
            builder.WithTimestamp(tsColumn);

        var keys = _model.AdditionalSettings.TryGetValue("keys", out var kObj) && kObj is string[] kArr
            ? kArr
            : _model.KeyProperties.Select(p => p.Name).ToArray();

        var projection = _model.AdditionalSettings.TryGetValue("projection", out var pObj) && pObj is string[] pArr
            ? pArr
            : _model.AllProperties.Select(p => p.Name).ToArray();

        var order = keys.Concat(projection.Where(p => !keys.Contains(p))).ToList();
        foreach (var name in order)
        {
            var property = _model.EntityType.GetProperty(name);
            if (property == null) continue;
            var meta = PropertyMeta.FromProperty(property);
            var columnName = KsqlNameUtils.Sanitize(meta.Name);
            var type = Schema.KsqlTypeMapping.MapToKsqlType(meta.PropertyType, meta.PropertyInfo, meta.Precision, meta.Scale);
            builder.AddColumn(columnName, type, keys.Contains(name));
        }

        return builder.Build();
    }
}
