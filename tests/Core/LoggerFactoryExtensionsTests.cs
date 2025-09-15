using Kafka.Ksql.Linq.Core.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.IO;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Core;

public class LoggerFactoryExtensionsTests
{
    [Fact]
    public void CreateLoggerOrNull_ReturnsNullLoggerWhenFactoryNull()
    {
        ILogger<LoggerFactoryExtensionsTests> logger = ((ILoggerFactory?)null).CreateLoggerOrNull<LoggerFactoryExtensionsTests>();
        Assert.Same(NullLogger<LoggerFactoryExtensionsTests>.Instance, logger);
    }

    [Fact]
    public void CreateLoggerOrNull_UsesFactory()
    {
        using var factory = LoggerFactory.Create(builder => { });
        var logger = factory.CreateLoggerOrNull<LoggerFactoryExtensionsTests>();
        Assert.NotNull(logger);
    }

    [Fact]
    public void LogMethods_WithAndWithoutFactory()
    {
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(NullLoggerProvider.Instance));
        var logger = factory.CreateLogger<LoggerFactoryExtensionsTests>();
        logger.LogDebugWithLegacySupport(factory, false, "d");
        logger.LogInformationWithLegacySupport(factory, false, "i");
        logger.LogWarningWithLegacySupport(factory, false, "w");
        logger.LogErrorWithLegacySupport(new Exception("e"), factory, false, "e");

        var sw = new StringWriter();
        Console.SetOut(sw);
        logger.LogDebugWithLegacySupport(null, true, "{0}", 1);
        logger.LogInformationWithLegacySupport(null, true, "{0}", 2);
        logger.LogWarningWithLegacySupport(null, true, "{0}", 3);
        logger.LogErrorWithLegacySupport(new Exception("err"), null, true, "{0}", 4);
        var output = sw.ToString();
        Assert.Contains("[DEBUG]", output);
        Assert.Contains("[INFO]", output);
        Assert.Contains("[WARNING]", output);
        Assert.Contains("[ERROR]", output);
    }
}
