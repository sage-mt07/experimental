using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Kafka.Ksql.Linq;

public abstract partial class KsqlContext
{
    public IEntitySet<T> Set<T>() where T : class
    {
        var entityType = typeof(T);

        if (_entitySets.TryGetValue(entityType, out var existingSet))
        {
            return (IEntitySet<T>)existingSet;
        }

        var entityModel = GetOrCreateEntityModel<T>();
        var entitySet = CreateEntitySet<T>(entityModel);
        _entitySets[entityType] = entitySet;

        return entitySet;
    }

    public object GetEventSet(Type entityType)
    {
        if (_entitySets.TryGetValue(entityType, out var entitySet))
        {
            return entitySet;
        }

        var entityModel = GetOrCreateEntityModel(entityType);
        var createdSet = CreateEntitySet(entityType, entityModel);
        _entitySets[entityType] = createdSet;

        return createdSet;
    }

    public Dictionary<Type, EntityModel> GetEntityModels()
    {
        return new Dictionary<Type, EntityModel>(_entityModels);
    }

    public IReadOnlyDictionary<Type, Configuration.ResolvedEntityConfig> GetResolvedEntityConfigs()
    {
        return _resolvedConfigs;
    }

    private void InitializeEventSetProperties(ModelBuilder builder)
    {
        var contextType = GetType();
        var eventSetProps = contextType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.CanWrite
                && p.PropertyType.IsGenericType
                && p.PropertyType.GetGenericTypeDefinition() == typeof(EventSet<>));

        foreach (var prop in eventSetProps)
        {
            if (prop.GetValue(this) != null)
                continue;

            var entityType = prop.PropertyType.GetGenericArguments()[0];
            builder.AddEntityModel(entityType);
            var model = EnsureEntityModel(entityType);
            var set = CreateEntitySet(entityType, model);
            _entitySets[entityType] = set;
            prop.SetValue(this, set);
        }
    }

    protected virtual object CreateEntitySet(Type entityType, EntityModel entityModel)
    {
        var method = GetType()
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .FirstOrDefault(m =>
                m.Name == nameof(CreateEntitySet)
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == 1
                && m.GetParameters().Length == 1
                && m.GetParameters()[0].ParameterType == typeof(EntityModel)
            );

        if (method == null)
            throw new InvalidOperationException("Generic CreateEntitySet<T>(EntityModel) not found!");

        // After this
        var genericMethod = method.MakeGenericMethod(entityType);
        return genericMethod.Invoke(this, new object[] { entityModel })!;
    }

    protected void ConfigureModel()
    {
        var modelBuilder = new ModelBuilder(ValidationMode.Strict);
        InitializeEventSetProperties(modelBuilder);
        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            OnModelCreating(modelBuilder);
        }
        ApplyModelBuilderSettings(modelBuilder);
    }

    private void ResolveEntityConfigurations()
    {
        _resolvedConfigs.Clear();

        foreach (var (type, model) in _entityModels)
        {
            var config = _dslOptions.Entities.FirstOrDefault(e => string.Equals(e.Entity, type.Name, StringComparison.OrdinalIgnoreCase));

            var defaultTopic = model.TopicName ?? type.Name.ToLowerInvariant();
            var sourceTopic = config?.SourceTopic ?? defaultTopic;

            var defaultCache = model.EnableCache;
            bool enableCache = false;
            if (model.StreamTableType == StreamTableType.Table)
            {
                enableCache = config?.EnableCache ?? defaultCache;
            }

            var defaultStore = model.AdditionalSettings.TryGetValue("StoreName", out var sObj) ? sObj?.ToString() : null;
            var storeName = config?.StoreName ?? defaultStore;

            string? groupId = null;
            if (_dslOptions.Topics.TryGetValue(sourceTopic, out var topicSection) && !string.IsNullOrEmpty(topicSection.Consumer.GroupId))
            {
                groupId = topicSection.Consumer.GroupId;
                if (!string.IsNullOrEmpty(model.GroupId) && model.GroupId != groupId)
                {
                    _logger.LogWarning("GroupId for {Entity} overridden by configuration: {Config} (was {Dsl})", type.Name, groupId, model.GroupId);
                }
            }
            else if (!string.IsNullOrEmpty(model.GroupId))
            {
                groupId = model.GroupId;
            }

            if (config != null && config.EnableCache != defaultCache)
            {
                _logger.LogInformation("EnableCache for {Entity} set to {Value} from configuration", type.Name, enableCache);
            }

            var resolved = new Configuration.ResolvedEntityConfig
            {
                Entity = type.Name,
                SourceTopic = sourceTopic,
                GroupId = groupId,
                EnableCache = enableCache,
                StoreName = storeName
            };

            foreach (var kv in model.AdditionalSettings)
            {
                resolved.AdditionalSettings[kv.Key] = kv.Value;
            }
            if (config?.BaseDirectory != null)
            {
                resolved.AdditionalSettings["BaseDirectory"] = config.BaseDirectory;
            }

            _resolvedConfigs[type] = resolved;
        }

        _dslOptions.Entities.Clear();
        foreach (var rc in _resolvedConfigs.Values)
        {
            _dslOptions.Entities.Add(new EntityConfiguration
            {
                Entity = rc.Entity,
                SourceTopic = rc.SourceTopic,
                EnableCache = rc.EnableCache,
                StoreName = rc.StoreName,
                BaseDirectory = rc.AdditionalSettings.TryGetValue("BaseDirectory", out var bd) ? bd?.ToString() : null
            });
        }
    }

    private void InitializeEntityModels()
    {
        using (Kafka.Ksql.Linq.Core.Modeling.ModelCreatingScope.Enter())
        {
            var dlqModel = CreateEntityModelFromType(typeof(Messaging.DlqEnvelope));
            dlqModel.SetStreamTableType(Query.Abstractions.StreamTableType.Stream);
            dlqModel.TopicName = GetDlqTopicName();
            dlqModel.AccessMode = Core.Abstractions.EntityAccessMode.ReadOnly;
            _entityModels[typeof(Messaging.DlqEnvelope)] = dlqModel;
            var ns = dlqModel.AdditionalSettings.TryGetValue("namespace", out var n) ? n?.ToString() : null;
            _mappingRegistry.RegisterEntityModel(dlqModel, overrideNamespace: ns);
        }
    }

    private void ApplyModelBuilderSettings(ModelBuilder modelBuilder)
    {
        var models = modelBuilder.GetAllEntityModels();
        foreach (var (type, model) in models)
        {
            if (model.QueryModel != null)
            {
                RegisterQueryModelMapping(model);
                _entityModels[type] = model;
                continue;
            }

            if (_entityModels.TryGetValue(type, out var existing))
            {
                existing.SetStreamTableType(model.GetExplicitStreamTableType());
                existing.ErrorAction = model.ErrorAction;
                existing.DeserializationErrorPolicy = model.DeserializationErrorPolicy;
                existing.EnableCache = model.EnableCache;
                existing.BarTimeSelector = model.BarTimeSelector;
            }
            else
            {
                _entityModels[type] = model;
            }

            var ns = model.AdditionalSettings.TryGetValue("namespace", out var n1) ? n1?.ToString() : null;
            _mappingRegistry.RegisterEntityModel(model, genericValue: model.StreamTableType == StreamTableType.Table, overrideNamespace: ns);
        }
    }

    private EntityModel GetOrCreateEntityModel<T>() where T : class
    {
        return GetOrCreateEntityModel(typeof(T));
    }

    private EntityModel GetOrCreateEntityModel(Type entityType)
    {
        if (_entityModels.TryGetValue(entityType, out var existingModel))
        {
            return existingModel;
        }

        var entityModel = CreateEntityModelFromType(entityType);
        _entityModels[entityType] = entityModel;
        return entityModel;
    }

    private EntityModel CreateEntityModelFromType(Type entityType)
    {
        var allProperties = entityType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
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

        if (model.StreamTableType == StreamTableType.Stream)
        {
            model.EnableCache = false;
        }
        else
        {
            model.EnableCache = true;
        }

        var topicAttr = entityType.GetCustomAttribute<KsqlTopicAttribute>();
        if (topicAttr != null)
        {
            model.TopicName = topicAttr.Name;
            model.Partitions = topicAttr.PartitionCount;
            model.ReplicationFactor = topicAttr.ReplicationFactor;
        }

        if (entityType == typeof(Messaging.DlqEnvelope))
        {
            model.TopicName = _dslOptions.DlqTopicName;
            model.Partitions = _dslOptions.DlqOptions.NumPartitions;
            model.ReplicationFactor = _dslOptions.DlqOptions.ReplicationFactor;
        }
        else
        {
            var config = _dslOptions.Entities.FirstOrDefault(e =>
                string.Equals(e.Entity, entityType.Name, StringComparison.OrdinalIgnoreCase));

            if (config != null)
            {
                if (!string.IsNullOrEmpty(config.SourceTopic))
                    model.TopicName = config.SourceTopic;

                if (model.StreamTableType == StreamTableType.Table)
                    model.EnableCache = config.EnableCache;


                if (config.StoreName != null)
                    model.AdditionalSettings["StoreName"] = config.StoreName;

                if (config.BaseDirectory != null)
                    model.AdditionalSettings["BaseDirectory"] = config.BaseDirectory;
            }
        }

        var validation = new ValidationResult { IsValid = true };
        if (keyProperties.Length == 0)
        {
            validation.Warnings.Add($"No key properties defined for {entityType.Name}");
        }
        model.ValidationResult = validation;

        return model;
    }

    internal EntityModel EnsureEntityModel(Type entityType, EntityModel? model = null)
    {
        if (_entityModels.TryGetValue(entityType, out var existing))
            return existing;

        model ??= CreateEntityModelFromType(entityType);
        _entityModels[entityType] = model;
        var ns2 = model.AdditionalSettings.TryGetValue("namespace", out var n2) ? n2?.ToString() : null;
        _mappingRegistry.RegisterEntityModel(model, genericValue: model.StreamTableType == StreamTableType.Table, overrideNamespace: ns2);

        return model;
    }
}
