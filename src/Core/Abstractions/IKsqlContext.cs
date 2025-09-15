//using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Core.Dlq;
using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Core.Abstractions;

/// <summary>
/// Abstract definition of KsqlContext
/// Unified interface similar to DbContext
/// </summary>
public interface IKsqlContext : IDisposable, IAsyncDisposable
{
    IEntitySet<T> Set<T>() where T : class;
    object GetEventSet(Type entityType);

    Dictionary<Type, EntityModel> GetEntityModels();

    IDlqClient Dlq => throw new NotImplementedException();

}