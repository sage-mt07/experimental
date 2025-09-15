using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
namespace Kafka.Ksql.Linq.Core.Modeling;
internal class ModelBuilder : IModelBuilder
{
    private readonly Dictionary<Type, EntityModel> _entityModels = new();
    private readonly ValidationMode _validationMode;

    public ModelBuilder(ValidationMode validationMode = ValidationMode.Strict)
    {
        _validationMode = validationMode;
    }
    public EntityModelBuilder<T> Entity<T>(bool readOnly = false, bool writeOnly = false) where T : class
    {
        if (readOnly && writeOnly)
            throw new ArgumentException("Cannot specify both readOnly and writeOnly");

        AddEntityModel<T>();
        var model = GetEntityModel<T>();
        if (model == null)
        {
            throw new InvalidOperationException($"Failed to create entity model for {typeof(T).Name}");
        }

        model.AccessMode = readOnly ? EntityAccessMode.ReadOnly :
                           writeOnly ? EntityAccessMode.WriteOnly :
                           EntityAccessMode.ReadWrite;

        return new EntityModelBuilder<T>(model, this);
    }
    public EntityModel? GetEntityModel<T>() where T : class
    {
        return GetEntityModel(typeof(T));
    }

    public EntityModel? GetEntityModel(Type entityType)
    {
        _entityModels.TryGetValue(entityType, out var model);
        return model;
    }

    public void AddEntityModel<T>() where T : class
    {
        AddEntityModel(typeof(T));
    }

    public void AddEntityModel(Type entityType)
    {
        if (_entityModels.ContainsKey(entityType))
            return;

        var entityModel = CreateEntityModelFromType(entityType);
        _entityModels[entityType] = entityModel;
    }

    public void RemoveEntityModel<T>() where T : class
    {
        RemoveEntityModel(typeof(T));
    }

    public void RemoveEntityModel(Type entityType)
    {
        _entityModels.Remove(entityType);
    }

    public Dictionary<Type, EntityModel> GetAllEntityModels()
    {
        return new Dictionary<Type, EntityModel>(_entityModels);
    }

    public string GetModelSummary()
    {
        if (_entityModels.Count == 0)
            return "ModelBuilder: No entities configured";

        var summary = new List<string>
            {
                $"ModelBuilder: {_entityModels.Count} entities configured",
                $"Validation Mode: {_validationMode}",
                ""
            };

        foreach (var (entityType, model) in _entityModels.OrderBy(x => x.Key.Name))
        {
            var status = model.IsValid ? "✅" : "❌";
            summary.Add($"{status} {entityType.Name} → {model.GetTopicName()} ({model.StreamTableType}, Keys: {model.KeyProperties.Length})");

            if (model.ValidationResult != null && !model.ValidationResult.IsValid)
            {
                foreach (var error in model.ValidationResult.Errors)
                {
                    summary.Add($"   Error: {error}");
                }
            }

            if (model.ValidationResult != null && model.ValidationResult.Warnings.Count > 0)
            {
                foreach (var warning in model.ValidationResult.Warnings)
                {
                    summary.Add($"   Warning: {warning}");
                }
            }
        }

        return string.Join(Environment.NewLine, summary);
    }

    public bool ValidateAllModels()
    {
        bool allValid = true;

        foreach (var (entityType, model) in _entityModels)
        {
            var validation = ValidateEntityModel(entityType, model);
            model.ValidationResult = validation;

            if (!validation.IsValid)
            {
                allValid = false;
                if (_validationMode == ValidationMode.Strict)
                {
                    throw new InvalidOperationException($"Entity model validation failed for {entityType.Name}: {string.Join(", ", validation.Errors)}");
                }
            }
        }

        return allValid;
    }

    private EntityModel CreateEntityModelFromType(Type entityType)
    {
        var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var keyProperties = allProperties
            .Select(p => new { Property = p, Attr = p.GetCustomAttribute<KsqlKeyAttribute>() })
            .Where(x => x.Attr != null)
            .OrderBy(x => x.Attr!.Order)
            .Select(x => x.Property)
            .ToArray();

        var model = new EntityModel
        {
            EntityType = entityType,
            TopicName = entityType.Name.ToLowerInvariant(),
            Partitions = 1,
            ReplicationFactor = 1,
            AllProperties = allProperties,
            KeyProperties = keyProperties
        };

        if (entityType.GetCustomAttribute<KsqlTableAttribute>() != null)
        {
            model.SetStreamTableType(StreamTableType.Table);
        }

        var topicAttr = entityType.GetCustomAttribute<KsqlTopicAttribute>();
        if (topicAttr != null)
        {
            model.TopicName = topicAttr.Name;
            model.Partitions = topicAttr.PartitionCount;
            model.ReplicationFactor = topicAttr.ReplicationFactor;
        }

        // Perform validation
        model.ValidationResult = ValidateEntityModel(entityType, model);

        return model;
    }

    private ValidationResult ValidateEntityModel(Type entityType, EntityModel model)
    {
        var result = new ValidationResult { IsValid = true };

        // Basic validation of the entity type
        if (!entityType.IsClass || entityType.IsAbstract)
        {
            result.IsValid = false;
            result.Errors.Add($"Entity type {entityType.Name} must be a concrete class");
        }


        // Validate properties
        foreach (var property in model.AllProperties)
        {
            var underlying = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

            if (underlying == typeof(char))
            {
                result.Warnings.Add($"Property {property.Name} is of type char which may not map cleanly to KSQL. Consider using string instead.");
            }
            else if (!IsValidPropertyType(property.PropertyType))
            {
                result.Warnings.Add($"Property {property.Name} has potentially unsupported type {property.PropertyType.Name}");
            }
        }

        // Validate key properties
        if (model.KeyProperties.Length == 0)
        {
            result.Warnings.Add($"Entity {entityType.Name} has no key properties, will be treated as Stream");
        }

        return result;
    }

    private bool IsValidPropertyType(Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        return underlyingType.IsPrimitive ||
               underlyingType == typeof(string) ||
               underlyingType == typeof(decimal) ||
               underlyingType == typeof(DateTime) ||
               underlyingType == typeof(DateTimeOffset) ||
               underlyingType == typeof(Guid) ||
               underlyingType == typeof(byte[]);
    }
}
