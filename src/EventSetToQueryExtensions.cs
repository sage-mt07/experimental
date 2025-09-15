using Kafka.Ksql.Linq.Query.Dsl;
using System;

namespace Kafka.Ksql.Linq;

/// <summary>
/// Provides ToQuery() extension on EventSet for defining view entities
/// using the KsqlQuery DSL.
/// </summary>
public static class EventSetToQueryExtensions
{
    public static EventSet<T> ToQuery<T>(this EventSet<T> set, Func<KsqlQueryRoot, IKsqlQueryable> build)
        where T : class
    {
        if (set == null) throw new ArgumentNullException(nameof(set));
        if (build == null) throw new ArgumentNullException(nameof(build));

        var root = new KsqlQueryRoot();
        var query = build(root) ?? throw new InvalidOperationException("Query builder returned null");
        var model = query.Build();

        // Validate that projection result type matches T
        if (model.SelectProjection != null && model.SelectProjection.ReturnType != typeof(T))
        {
            throw new InvalidOperationException("Select projection type must match EventSet type.");
        }

        ToQueryValidator.ValidateSelectMatchesPoco(typeof(T), model);

        var entityModel = set.GetEntityModel();
        entityModel.QueryModel = model;
        return set;
    }
}

