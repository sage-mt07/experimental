using System;
using System.Reflection;

#nullable enable

namespace Kafka.Ksql.Linq.Tests;

internal static class PrivateAccessor
{
    internal static object? InvokePrivate(
        object target,
        string name,
        Type[] parameterTypes,
        Type[]? genericTypes = null,
        params object?[]? args)
    {
        var startType = target as Type ?? target.GetType();
        var type = startType;
        var flags = BindingFlags.NonPublic | BindingFlags.Public |
                    (target is Type ? BindingFlags.Static : BindingFlags.Instance);
        MethodInfo? method = null;
        while (type != null)
        {
            method = type.GetMethod(name, flags, binder: null, types: parameterTypes, modifiers: null);
            if (method != null)
                break;
            type = type.BaseType;
        }
        if (method == null)
            throw new ArgumentException($"Method '{name}' with specified parameters not found on type '{startType.FullName}'.");
        if (genericTypes != null && method.IsGenericMethodDefinition)
        {
            method = method.MakeGenericMethod(genericTypes);
        }
        try
        {
            return method.Invoke(target is Type ? null : target, args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            // Unwrap the inner exception so callers can assert on the actual
            // exception type thrown by the invoked member.
            throw ex.InnerException;
        }
    }

    internal static T InvokePrivate<T>(
        object target,
        string name,
        Type[] parameterTypes,
        Type[]? genericTypes = null,
        params object?[]? args) => (T)InvokePrivate(target, name, parameterTypes, genericTypes, args)!;
}
