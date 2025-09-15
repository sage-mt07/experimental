using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kafka.Ksql.Linq.Query.Dsl;

internal static class ToQueryValidator
{
    public static void ValidateSelectMatchesPoco(Type resultType, KsqlQueryModel model)
    {
        if (resultType == null) throw new ArgumentNullException(nameof(resultType));
        if (model == null) throw new ArgumentNullException(nameof(model));

        var entityProps = resultType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.MetadataToken)
            .Where(p => !Attribute.IsDefined(p, typeof(KsqlIgnoreAttribute), true))
            .ToArray();

        var entityPropMap = entityProps.ToDictionary(p => p.Name);

        var projectionProps = ExtractProjectionProperties(model.SelectProjection, resultType)
            .Where(p => entityPropMap.ContainsKey(p.Prop.Name))
            .ToArray();

        if (entityProps.Length != projectionProps.Length)
            throw new InvalidOperationException("Select projection does not match POCO properties.");

        for (int i = 0; i < entityProps.Length; i++)
        {
            var proj = projectionProps[i];
            if (entityProps[i].Name != proj.Prop.Name)
                throw new InvalidOperationException("Select projection does not match POCO property order.");
            if (entityProps[i].PropertyType != proj.Type)
                throw new InvalidOperationException("Select projection property types do not match POCO.");
            if ((entityProps[i].PropertyType == typeof(decimal) || entityProps[i].PropertyType == typeof(decimal?))
                && (proj.Type == typeof(decimal) || proj.Type == typeof(decimal?)) && proj.Source != null)
            {
                var ea = entityProps[i].GetCustomAttribute<KsqlDecimalAttribute>(true);
                var sa = proj.Source.GetCustomAttribute<KsqlDecimalAttribute>(true);
                var ep = DecimalPrecisionConfig.ResolvePrecision(ea?.Precision, entityProps[i]);
                var es = DecimalPrecisionConfig.ResolveScale(ea?.Scale, entityProps[i]);
                var sp = DecimalPrecisionConfig.ResolvePrecision(sa?.Precision, proj.Source);
                var ss = DecimalPrecisionConfig.ResolveScale(sa?.Scale, proj.Source);
                if (ep != sp || es != ss)
                    throw new InvalidOperationException("Select projection decimal precision does not match POCO.");
            }
        }

        var entityKeys = entityProps
            .Select(p => (Prop: p, Attr: p.GetCustomAttribute<KsqlKeyAttribute>(true)))
            .Where(x => x.Attr != null)
            .OrderBy(x => x.Attr!.Order)
            .Select(x => x.Prop.Name)
            .ToArray();

        var projectionKeys = projectionProps
            .Select(p => (Name: p.Prop.Name, Attr: entityPropMap.TryGetValue(p.Prop.Name, out var ep)
                ? ep.GetCustomAttribute<KsqlKeyAttribute>(true)
                : null))
            .Where(x => x.Attr != null)
            .OrderBy(x => x.Attr!.Order)
            .Select(x => x.Name)
            .ToArray();

        if (!entityKeys.SequenceEqual(projectionKeys))
            throw new InvalidOperationException("Select projection key order does not match POCO.");
    }

    private static List<(PropertyInfo Prop, PropertyInfo? Source, Type Type)> ExtractProjectionProperties(LambdaExpression? projection, Type resultType)
    {
        if (projection == null)
            return resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .OrderBy(p => p.MetadataToken)
                .Select(p => (Prop: p, Source: (PropertyInfo?)p, Type: p.PropertyType))
                .ToList();

        var props = new List<(PropertyInfo, PropertyInfo?, Type)>();
        switch (projection.Body)
        {
            case NewExpression newExpr when newExpr.Members != null:
                for (int i = 0; i < newExpr.Members.Count; i++)
                {
                    if (newExpr.Members[i] is PropertyInfo mem && resultType.GetProperty(mem.Name) != null)
                    {
                        PropertyInfo? src = (newExpr.Arguments[i] as MemberExpression)?.Member as PropertyInfo;
                        props.Add((mem, src, newExpr.Arguments[i].Type));
                    }
                }
                break;
            case MemberInitExpression initExpr:
                foreach (var binding in initExpr.Bindings.OfType<MemberAssignment>())
                {
                    if (binding.Member is PropertyInfo mem && resultType.GetProperty(mem.Name) != null)
                    {
                        PropertyInfo? src = (binding.Expression as MemberExpression)?.Member as PropertyInfo;
                        props.Add((mem, src, binding.Expression.Type));
                    }
                }
                break;
            case ParameterExpression:
                props.AddRange(resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .OrderBy(p => p.MetadataToken)
                    .Select(p => (p, (PropertyInfo?)p, p.PropertyType)));
                break;
            case MemberExpression me when me.Member is PropertyInfo pi:
                if (resultType.GetProperty(pi.Name) != null)
                    props.Add((pi, me.Member as PropertyInfo, me.Type));
                break;
        }
        return props;
    }
}
