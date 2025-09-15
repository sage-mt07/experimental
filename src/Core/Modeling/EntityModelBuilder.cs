using Kafka.Ksql.Linq.Core.Abstractions;
using System;


namespace Kafka.Ksql.Linq.Core.Modeling;

public class EntityModelBuilder<T> where T : class
{
    private readonly EntityModel _entityModel;
    internal ModelBuilder Owner { get; }

    internal EntityModelBuilder(EntityModel entityModel, ModelBuilder owner)
    {
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
        Owner = owner;
    }
    public EntityModelBuilder<T> OnError(ErrorAction action)
    {
        _entityModel.ErrorAction = action;
        return this;
    }

    public EntityModel GetModel()
    {
        return _entityModel;
    }

    [Obsolete("Changing the topic name via Fluent API is prohibited in POCO attribute-driven mode. Use the [Topic] attribute instead.", true)]
    public EntityModelBuilder<T> HasTopicName(string topicName)
    {
        throw new NotSupportedException("Changing the topic name via Fluent API is prohibited in POCO attribute-driven mode. Use the [Topic] attribute instead.");
    }

    public override string ToString()
    {
        var entityName = _entityModel.EntityType.Name;
        var topicName = _entityModel.TopicName ?? "undefined";
        var keyCount = _entityModel.KeyProperties.Length;
        var validStatus = _entityModel.IsValid ? "valid" : "invalid";

        return $"Entity: {entityName}, Topic: {topicName}, Keys: {keyCount}, Status: {validStatus}";
    }

}
