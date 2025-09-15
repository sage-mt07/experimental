using Kafka.Ksql.Linq.Query.Dsl;
using System;

namespace Kafka.Ksql.Linq.Core.Modeling;

/// <summary>
/// Extensions for defining query-based entities using the new ToQuery DSL.
/// </summary>
public static class EntityBuilderToQueryExtensions
{
    public static EntityModelBuilder<T> ToQuery<T>(this EntityModelBuilder<T> builder, Func<KsqlQueryRoot, IKsqlQueryable> build)
        where T : class
    {
        if (builder == null) throw new ArgumentNullException(nameof(builder));
        if (build == null) throw new ArgumentNullException(nameof(build));

        var root = new KsqlQueryRoot();
        var query = build(root) ?? throw new InvalidOperationException("Query builder returned null");
        var model = query.Build();

        if (model.SourceTypes.Length == 0 || model.SourceTypes.Length > 2)
            throw new NotSupportedException("Only 1 or 2 source types are supported in this phase.");

        ToQueryValidator.ValidateSelectMatchesPoco(typeof(T), model);

        builder.GetModel().QueryModel = model;
        return builder;
    }
}
