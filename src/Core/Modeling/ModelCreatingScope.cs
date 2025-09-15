using System;
using System.Threading;

namespace Kafka.Ksql.Linq.Core.Modeling;

internal static class ModelCreatingScope
{
    private static readonly AsyncLocal<bool> _inModelCreating = new();

    public static bool IsActive => _inModelCreating.Value;

    public static IDisposable Enter()
    {
        _inModelCreating.Value = true;
        return new ScopeToken();
    }

    private sealed class ScopeToken : IDisposable
    {
        private bool _disposed;
        public void Dispose()
        {
            if (!_disposed)
            {
                _inModelCreating.Value = false;
                _disposed = true;
            }
        }
    }

    public static void EnsureInScope()
    {
        if (!IsActive)
            throw new InvalidOperationException("Where/GroupBy/Select query definitions are allowed only inside OnModelCreating.");
    }
}
