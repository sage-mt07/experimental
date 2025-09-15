//using Kafka.Ksql.Linq.Core.Configuration;
//using Kafka.Ksql.Linq.Configuration;
//using System;
//using Xunit;

//namespace Kafka.Ksql.Linq.Tests.Core;

//public class CoreSettingsProviderTests
//{
//    [Fact]
//    public void GetSettings_ReturnsClone()
//    {
//        var provider = new CoreSettingsProvider();
//        var settings = provider.GetSettings();
//        Assert.NotSame(settings, provider.GetSettings());
//        Assert.Equal(ValidationMode.Strict, settings.ValidationMode);
//    }

//    [Fact]
//    public void UpdateSettings_Null_Throws()
//    {
//        var provider = new CoreSettingsProvider();
//        Assert.Throws<ArgumentNullException>(() => provider.UpdateSettings(null!));
//    }

//    [Fact]
//    public void UpdateSettings_RaisesEventAndClones()
//    {
//        var provider = new CoreSettingsProvider();
//        var raised = false;
//        provider.SettingsChanged += (s, e) =>
//        {
//            raised = true;
//            Assert.Equal(ValidationMode.Strict, e.OldSettings.ValidationMode);
//            Assert.Equal(ValidationMode.Relaxed, e.NewSettings.ValidationMode);
//        };
//        var newSettings = new CoreSettings
//        {
//            ValidationMode = ValidationMode.Relaxed,
//            KafkaBootstrapServers = "localhost:9092",
//            ApplicationId = "app",
//            StateStoreDirectory = "/tmp"
//        };
//        provider.UpdateSettings(newSettings);
//        Assert.True(raised);
//        var retrieved = provider.GetSettings();
//        Assert.Equal(ValidationMode.Relaxed, retrieved.ValidationMode);
//        Assert.NotSame(newSettings, retrieved);
//    }
//}
