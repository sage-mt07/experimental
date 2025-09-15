using System;
using System.ComponentModel;
using System.Reflection;

namespace Kafka.Ksql.Linq.Configuration;
public static class DefaultValueBinder
{
    public static void ApplyDefaults(object target)
    {
        if (target is null) return;
        var type = target.GetType();
        foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!prop.CanWrite) continue;
            var attr = prop.GetCustomAttribute<DefaultValueAttribute>();
            if (attr is null) continue;
            var current = prop.GetValue(target);
            bool isUnset = current is null ||
                (prop.PropertyType.IsValueType && Equals(current, Activator.CreateInstance(prop.PropertyType))) ||
                (current is string s && s == string.Empty);
            if (isUnset)
            {
                var value = attr.Value;
                if (value is null) continue;
                if (value is Type t)
                {
                    value = Activator.CreateInstance(t)!;
                }
                prop.SetValue(target, value);
                current = value;
            }
            if (current is not null && !prop.PropertyType.IsValueType)
            {
                ApplyDefaults(current);
            }
        }
    }
}
