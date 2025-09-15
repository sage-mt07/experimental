using Confluent.Kafka;
using Kafka.Ksql.Linq.Cache.Core;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using Kafka.Ksql.Linq.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Cache.Extensions;

internal static class KsqlContextCacheExtensions
{
    private static readonly Dictionary<IKsqlContext, TableCacheRegistry> _registries = new();
    private static readonly object _lock = new();

    internal static void UseTableCache(this IKsqlContext context, KsqlDslOptions options, ILoggerFactory? loggerFactory = null)
    {
        lock (_lock)
        {
            loggerFactory ??= NullLoggerFactory.Instance;
            if (_registries.ContainsKey(context))
                return;

            var mapping = ((KsqlContext)context).GetMappingRegistry();
            var models = context.GetEntityModels();
            if (!options.Entities.Any(e => e.EnableCache))
                return;

            var bootstrap = options.Common.BootstrapServers;
            var appIdBase = options.Common.ApplicationId;
            var schemaUrl = options.SchemaRegistry.Url;
            var registry = new TableCacheRegistry();

            foreach (var e in options.Entities.Where(e => e.EnableCache))
            {
                var model = models.Values.FirstOrDefault(m => string.Equals(m.EntityType.Name, e.Entity, StringComparison.OrdinalIgnoreCase));
                if (model == null)
                    continue;

                var kv = mapping.GetMapping(model.EntityType);
                var storeName = e.StoreName ?? model.GetTopicName();
                var topic = model.GetTopicName();
                var applicationId = $"{appIdBase}-{storeName}";
                var stateDir = Path.Combine(Path.GetTempPath(), applicationId);

                var builder = new StreamBuilder();
                var materialized = CreateStringKeyMaterializedGeneric(kv.AvroValueType!, storeName);
                StreamToStringKeyTableGeneric(builder, kv.AvroKeyType!, kv.AvroValueType!, topic, materialized, kv);

                var config = CreateStreamConfigGeneric(kv.AvroKeyType!, kv.AvroValueType!, applicationId, bootstrap, schemaUrl, stateDir, loggerFactory);
                var ks = new KafkaStream(builder.Build(), (IStreamConfig)config);
                var wait = CreateWaitUntilRunning(ks);
                var enumerateLazy = CreateEnumeratorLazyGeneric(typeof(string), kv.AvroValueType!, ks, storeName);

                var cache = CreateTableCacheGeneric(model.EntityType, mapping, storeName, wait, enumerateLazy);

                registry.Register(model.EntityType, cache);

                ks.StartAsync();
            }

            context.AttachTableCacheRegistry(registry);
        }
    }
    private static Func<TimeSpan?, Task> CreateWaitUntilRunning(KafkaStream stream)
    {
        var running = false;
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        stream.StateChanged += (_, s) =>
        {
            if (s == KafkaStream.State.RUNNING)
            {
                running = true;
                tcs.TrySetResult(true);
            }
        };
        return async (TimeSpan? timeout) =>
        {
            if (running) return;
            var task = tcs.Task;
            if (timeout.HasValue)
            {
                if (await Task.WhenAny(task, Task.Delay(timeout.Value)) != task)
                    throw new TimeoutException("KafkaStream failed to reach RUNNING state");
            }
            else
            {
                await task;
            }
        };
    }

    // Wrap ks.Store(...).All() in a type-safe enumerator function
    private static Lazy<Func<IEnumerable<(object key, object val)>>> CreateEnumeratorLazyGeneric(
        Type keyType, Type valueType, KafkaStream ks, string storeName)
    {
        var m = typeof(KsqlContextCacheExtensions)
                 .GetMethod(nameof(CreateEnumeratorLazy), BindingFlags.NonPublic | BindingFlags.Static)!;
        return (Lazy<Func<IEnumerable<(object key, object val)>>>)
                m.MakeGenericMethod(keyType, valueType)
             .Invoke(null, new object[] { ks, storeName })!;
    }

    private static Lazy<Func<IEnumerable<(object key, object val)>>> CreateEnumeratorLazy<TKey, TValue>(
        KafkaStream ks, string storeName)
        where TKey : class where TValue : class
    {
        return new Lazy<Func<IEnumerable<(object key, object val)>>>(() =>
        {
            var parameters = StoreQueryParameters.FromNameAndType(
                storeName, QueryableStoreTypes.KeyValueStore<TKey, TValue>());
            var store = ks.Store(parameters);
            var test = store.All();
            static IEnumerable<(object key, object val)> Enumerate(IReadOnlyKeyValueStore<TKey, TValue> s)
            {
                foreach (var it in s.All())
                {
                    yield return ((object)it.Key!, (object)it.Value!);
                }
            }
            return () => Enumerate(store);
        });
    }
    private static object CreateStreamConfigGeneric(Type keyType, Type valueType, string appId, string bootstrap, string schemaUrl, string stateDir, ILoggerFactory? loggerFactory)
    {
        var cfgType = typeof(StreamConfig<,>).MakeGenericType(
            typeof(SchemaAvroSerDes<>).MakeGenericType(keyType),
            typeof(SchemaAvroSerDes<>).MakeGenericType(valueType));
        dynamic cfg = Activator.CreateInstance(cfgType)!;
        cfg.ApplicationId = appId;
        cfg.BootstrapServers = bootstrap;
        cfg.SchemaRegistryUrl = schemaUrl;
        cfg.StateDir = stateDir;
        cfg.AutoOffsetReset = AutoOffsetReset.Earliest;
        cfg.Logger = loggerFactory;
        return cfg;
    }

    private static object CreateStringKeyMaterializedGeneric(Type valueType, string storeName)
    {
        var m = typeof(KsqlContextCacheExtensions)
            .GetMethod(nameof(CreateStringKeyMaterialized), BindingFlags.NonPublic | BindingFlags.Static)!;
        return m.MakeGenericMethod(valueType).Invoke(null, new object[] { storeName })!;
    }

    private static Materialized<string, TValue, IKeyValueStore<Bytes, byte[]>> CreateStringKeyMaterialized<TValue>(string storeName)
    {
        return Materialized<string, TValue, IKeyValueStore<Bytes, byte[]>>.Create<
            StringSerDes, SchemaAvroSerDes<TValue>>(storeName);
    }

    private static void StreamToStringKeyTableGeneric(
        StreamBuilder builder, Type keyType, Type valueType, string topic, object materialized, object mapping)
    {
        var m = typeof(KsqlContextCacheExtensions)
            .GetMethod(nameof(StreamToStringKeyTable), BindingFlags.NonPublic | BindingFlags.Static)!;
        m.MakeGenericMethod(keyType, valueType)
         .Invoke(null, new object[] { builder, topic, materialized, mapping });
    }

    private static void StreamToStringKeyTable<TKey, TValue>(
        StreamBuilder builder, string topic,
        Materialized<string, TValue, IKeyValueStore<Bytes, byte[]>> materialized,
        object mapping)
        where TKey : class where TValue : class
    {
        var formatKey = (Func<object, string>)(k =>
            (string)mapping.GetType().GetMethod("FormatKeyForPrefix")!.Invoke(mapping, new[] { k })!);

        var stream = builder.Stream<TKey, TValue>(topic);

        var withStringKey = stream.SelectKey(new Mapper<TKey, TValue>(k => formatKey(k)));

        var repartitioned = withStringKey.Repartition(
            Repartitioned<string, TValue>.As($"{topic}-by-stringkey")
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new SchemaAvroSerDes<TValue>()));

        _ = repartitioned.ToTable(materialized);
    }

    private class Mapper<TKeyLocal, TValueLocal> : IKeyValueMapper<TKeyLocal, TValueLocal, string>
    {
        private readonly Func<TKeyLocal, string> _f;
        public Mapper(Func<TKeyLocal, string> f) => _f = f;
        public string Apply(TKeyLocal key, TValueLocal value, IRecordContext context) => _f(key);
    }

    private static object CreateTableCacheGeneric(Type entityType, MappingRegistry mapping,
        string storeName, Func<TimeSpan?, Task> wait,
        Lazy<Func<IEnumerable<(object key, object val)>>> enumerateLazy)
    {
        var cacheType = typeof(TableCache<>).MakeGenericType(entityType);
        return Activator.CreateInstance(cacheType, mapping, storeName, wait, enumerateLazy)!;
    }

    internal static void AttachTableCacheRegistry(this IKsqlContext context, TableCacheRegistry registry)
    {
        _registries[context] = registry;
    }

    internal static TableCacheRegistry? GetTableCacheRegistry(this IKsqlContext context)
    {
        lock (_lock)
        {
            return _registries.TryGetValue(context, out var reg) ? reg : null;
        }
    }

    internal static ITableCache<T>? GetTableCache<T>(this IKsqlContext context) where T : class
    {
        var reg = context.GetTableCacheRegistry();
        return reg?.GetCache<T>();
    }
}
