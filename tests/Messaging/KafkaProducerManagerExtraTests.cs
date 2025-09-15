using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Configuration.Messaging;
using Kafka.Ksql.Linq.Core.Configuration;
using Kafka.Ksql.Linq.Mapping;
using Kafka.Ksql.Linq.Messaging.Producers;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Messaging;

public class KafkaProducerManagerExtraTests
{
    [Fact]
    public void CreateSchemaRegistryClient_UsesOptions()
    {
        var options = new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection
            {
                Url = "u",
                MaxCachedSchemas = 5,
                RequestTimeoutMs = 10,
                AdditionalProperties = new System.Collections.Generic.Dictionary<string, string> { { "p", "v" } },
                SslKeyPassword = "pw"
            }
        };
        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(options), null);
        var client = InvokePrivate<object>(manager, "CreateSchemaRegistryClient", System.Type.EmptyTypes);
        Assert.Equal("CachedSchemaRegistryClient", client!.GetType().Name);
    }

    [Fact(Skip = "Requires full configuration")]
    public void BuildProducerConfig_WithSecurityAndPartitioner()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection
            {
                BootstrapServers = "s",
                ClientId = "c",
                SecurityProtocol = Confluent.Kafka.SecurityProtocol.SaslSsl,
                SaslMechanism = Confluent.Kafka.SaslMechanism.Plain,
                SaslUsername = "u",
                SaslPassword = "p",
                SslCaLocation = "ca",
                SslCertificateLocation = "cert",
                SslKeyLocation = "key",
                SslKeyPassword = "kp"
            },
            Topics = new Dictionary<string, TopicSection>
            {
                ["t"] = new TopicSection
                {
                    Producer = new ProducerSection
                    {
                        Acks = "All",
                        CompressionType = "Gzip",
                        Partitioner = "m"
                    }
                }
            }
        };

        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(options), new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory());
        var config = InvokePrivate<Confluent.Kafka.ProducerConfig>(manager, "BuildProducerConfig", new[] { typeof(string) }, null, "t");

        Assert.Equal(Confluent.Kafka.SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(Confluent.Kafka.SaslMechanism.Plain, config.SaslMechanism);
        Assert.Equal("u", config.SaslUsername);
        Assert.Equal("p", config.SaslPassword);
        Assert.Equal("ca", config.SslCaLocation);
        Assert.Equal("cert", config.SslCertificateLocation);
        Assert.Equal("key", config.SslKeyLocation);
        Assert.Equal("kp", config.SslKeyPassword);
        var entries = (System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, string>>)config;
        Assert.Contains(entries, kv => kv.Key == "partitioner.class" && kv.Value == "m");
    }

    [Fact]
    public void CreateSchemaRegistryClient_WithAuthAndSsl()
    {
        var options = new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection
            {
                Url = "url",
                MaxCachedSchemas = 1,
                RequestTimeoutMs = 5,
                BasicAuthUserInfo = "x:y",
                BasicAuthCredentialsSource = BasicAuthCredentialsSource.UserInfo,
                SslCaLocation = "ca",
                SslKeystoreLocation = "ks",
                SslKeystorePassword = "pw",
                SslKeyPassword = "kp"
            }
        };

        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(options), null);
        var client = InvokePrivate<object>(manager, "CreateSchemaRegistryClient", System.Type.EmptyTypes);
        Assert.NotNull(client);
    }

    [Fact(Skip = "Requires internal serializer access")]
    public void SerializerCaching_WorksPerType()
    {
        var options = new KsqlDslOptions
        {
            SchemaRegistry = new SchemaRegistrySection { Url = "http://example" }
        };
        var manager = new KafkaProducerManager(new MappingRegistry(), Options.Create(options), null);
        var s1 = InvokePrivate<ISerializer<object>>(manager, "CreateKeySerializer", new[] { typeof(Type) }, null, typeof(int));
        var s2 = InvokePrivate<ISerializer<object>>(manager, "CreateKeySerializer", new[] { typeof(Type) }, null, typeof(int));
        var cache = (ConcurrentDictionary<Type, ISerializer<object>>)typeof(KafkaProducerManager)
            .GetField("_keySerializerCache", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;
        Assert.Same(s1, s2);
        Assert.Single(cache);

        var v1 = InvokePrivate<ISerializer<object>>(manager, "GetValueSerializer", System.Type.EmptyTypes, new[] { typeof(string) });
        var v2 = InvokePrivate<ISerializer<object>>(manager, "GetValueSerializer", System.Type.EmptyTypes, new[] { typeof(string) });
        var vcache = (ConcurrentDictionary<Type, ISerializer<object>>)typeof(KafkaProducerManager)
            .GetField("_valueSerializerCache", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;
        Assert.Same(v1, v2);
        Assert.Single(vcache);
    }
}
