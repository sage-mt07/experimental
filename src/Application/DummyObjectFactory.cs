using System;
using System.Reflection;

namespace Kafka.Ksql.Linq.Application;

internal static class DummyObjectFactory
{
    public static T CreateDummy<T>() where T : class, new()
    {
        var obj = new T();
        foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!prop.CanWrite) continue;
            var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            object? value = type.IsValueType ? Activator.CreateInstance(type) : "dummy";
            if (type == typeof(string)) value = "dummy";
            else if (type == typeof(bool)) value = true;
            else if (type == typeof(Guid)) value = Guid.NewGuid();
            else if (type == typeof(DateTime)) value = DateTime.UtcNow;
            else if (type == typeof(DateTimeOffset)) value = DateTimeOffset.UtcNow;
            else if (type.IsEnum) value = Enum.GetValues(type).GetValue(0);
            try { prop.SetValue(obj, value); } catch { }
        }
        return obj;
    }
}
