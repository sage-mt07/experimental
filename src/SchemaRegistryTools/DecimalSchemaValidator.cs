using Confluent.SchemaRegistry;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;

namespace Kafka.Ksql.Linq.SchemaRegistryTools;

public static class DecimalSchemaValidator
{
    public static void Validate(EntityModel model, ISchemaRegistryClient client, ValidationMode mode, ILogger logger)
    {
        var (_, valueMeta) = SchemaRegistryMetaProvider.GetMetaFromSchemaRegistry(model, client);
        var sr = valueMeta.ToDictionary(m => m.Name, m => (m.Precision, m.Scale));
        foreach (var prop in model.AllProperties)
        {
            if (prop.PropertyType != typeof(decimal)) continue;
            var meta = PropertyMeta.FromProperty(prop);
            var efP = DecimalPrecisionConfig.ResolvePrecision(meta.Precision, prop);
            var efS = DecimalPrecisionConfig.ResolveScale(meta.Scale, prop);
            if (!sr.TryGetValue(prop.Name, out var srVal) || srVal.Precision == null || srVal.Scale == null)
                continue;
            var srP = srVal.Precision!.Value;
            var srS = srVal.Scale!.Value;
            if (srP == efP && srS == efS) continue;
            var msg = $"DecimalPrecisionMismatch(entity={model.EntityType.Name}, property={prop.Name}, ef={efP},{efS}, sr={srP},{srS})";
            if (mode == ValidationMode.Strict)
                throw new InvalidOperationException(msg);
            logger.LogWarning(new EventId(1001, "DecimalPrecisionMismatch"), msg);
            logger.LogInformation(new EventId(1002, "DecimalScaleAdjusted"),
                $"DecimalScaleAdjusted(entity={model.EntityType.Name}, property={prop.Name}, from={efS} to={srS}, mode=Relaxed)");
            DecimalPrecisionConfig.Override(prop, srP, srS);
        }
    }
}
