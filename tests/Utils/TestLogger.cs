using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Tests.Utils;

public sealed class TestLoggerFactory : ILoggerFactory
{
    private readonly ConcurrentDictionary<string, TestLogger> _loggers = new();
    public IReadOnlyDictionary<string, TestLogger> Loggers => _loggers;

    public void AddProvider(ILoggerProvider provider) { }
    public ILogger CreateLogger(string categoryName)
        => _loggers.GetOrAdd(categoryName, _ => new TestLogger(categoryName));
    public void Dispose() { }
}

public sealed class TestLogger : ILogger
{
    public string Category { get; }
    public List<LogEntry> Entries { get; } = new();
    public TestLogger(string category) => Category = category;

    public IDisposable BeginScope<TState>(TState state) => NullScope.Instance;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        Entries.Add(new LogEntry
        {
            Level = logLevel,
            Message = formatter(state, exception),
            Exception = exception,
            EventId = eventId
        });
    }
}

public sealed class LogEntry
{
    public LogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public Exception? Exception { get; set; }
    public EventId EventId { get; set; }
}

file sealed class NullScope : IDisposable
{
    public static readonly NullScope Instance = new();
    public void Dispose() { }
}

