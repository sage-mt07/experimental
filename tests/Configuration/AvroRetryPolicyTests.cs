//using System;
//using Kafka.Ksql.Linq.Configuration.Options;
//using Xunit;

//namespace Kafka.Ksql.Linq.Tests.Configuration;

//public class AvroRetryPolicyTests
//{
//    [Fact]
//    public void Validate_InvalidMaxAttempts_Throws()
//    {
//        var policy = new AvroRetryPolicy { MaxAttempts = 0 };
//        Assert.Throws<ArgumentException>(() => policy.Validate());
//    }

//    [Fact]
//    public void Validate_InvalidDelays_Throws()
//    {
//        var policy = new AvroRetryPolicy { InitialDelay = System.TimeSpan.Zero };
//        Assert.Throws<ArgumentException>(() => policy.Validate());
//    }

//    [Theory]
//    [InlineData(0, 100, 100, 1)]
//    [InlineData(1, 0, 100, 1)]
//    [InlineData(1, 100, 0, 1)]
//    [InlineData(1, 100, 100, 0)]
//    public void Validate_InvalidValues_Throws(int attempts, int initMs, int maxMs, double backoff)
//    {
//        var policy = new AvroRetryPolicy
//        {
//            MaxAttempts = attempts,
//            InitialDelay = TimeSpan.FromMilliseconds(initMs),
//            MaxDelay = TimeSpan.FromMilliseconds(maxMs),
//            BackoffMultiplier = backoff
//        };
//        Assert.Throws<ArgumentException>(() => policy.Validate());
//    }
//}
