using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Ksql.Linq.Messaging.Consumers;

internal class ManualCommitManager : ICommitManager, EventSet<object>.ICommitRegistrar
{
    private record Binding(string Topic, object Consumer, System.Action<TopicPartitionOffset> Commit);
    private sealed class MetaBox { public MessageMeta Meta; public MetaBox(MessageMeta m) { Meta = m; } }

    private readonly Dictionary<Type, Binding> _bindings = new();
    private readonly ConditionalWeakTable<object, MetaBox> _meta = new();
    private readonly Dictionary<(string Topic, int Partition), SortedDictionary<long, WeakReference<object>>> _index = new();
    private readonly Dictionary<(string Topic, int Partition), long> _committed = new();
    private readonly object _lock = new();
    private readonly ILogger? _logger;

    public ManualCommitManager(ILogger? logger = null)
    {
        _logger = logger;
    }

    public void Bind(Type pocoType, string topic, object consumer)
    {
        if (pocoType == null) throw new ArgumentNullException(nameof(pocoType));
        if (topic == null) throw new ArgumentNullException(nameof(topic));
        if (consumer == null) throw new ArgumentNullException(nameof(consumer));
        // Resolve Commit(TopicPartitionOffset) via reflection to avoid dynamic binding issues
        System.Action<TopicPartitionOffset> commitAction = (tpo) =>
        {
            var type = consumer.GetType();
            var mi = type.GetMethod("Commit", new[] { typeof(System.Collections.Generic.IEnumerable<TopicPartitionOffset>) });
            object arg = new TopicPartitionOffset[] { tpo };
            if (mi == null)
            {
                // Fallback: try array-typed overload if present
                mi = type.GetMethod("Commit", new[] { typeof(TopicPartitionOffset[]) });
            }
            if (mi == null)
            {
                throw new MissingMethodException(type.FullName, "Commit(IEnumerable<TopicPartitionOffset>)");
            }
            mi.Invoke(consumer, new object[] { arg });
        };
        _bindings[pocoType] = new Binding(topic, consumer, commitAction);
        _logger?.LogInformation("ManualCommit bind: type={Type} topic={Topic}", pocoType.Name, topic);
    }

    void EventSet<object>.ICommitRegistrar.Track(object entity, MessageMeta meta)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        if (!_bindings.ContainsKey(entity.GetType()))
        {
            _logger?.LogDebug("ManualCommit track skipped: no binding for type={Type}", entity.GetType().Name);
            return;
        }
        _meta.Add(entity, new MetaBox(meta));
        _logger?.LogDebug("ManualCommit track: topic={Topic} p={Partition} off={Offset}", meta.Topic, meta.Partition, meta.Offset);
        try { Console.WriteLine($"[ManualCommit][track] topic={meta.Topic} p={meta.Partition} off={meta.Offset}"); } catch {}
        var key = (meta.Topic, meta.Partition);
        lock (_lock)
        {
            if (!_index.TryGetValue(key, out var dict))
                _index[key] = dict = new SortedDictionary<long, WeakReference<object>>();
            dict[meta.Offset] = new WeakReference<object>(entity);
        }
    }

    public void Commit(object entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        if (!_meta.TryGetValue(entity, out var box))
            return;
        var meta = box.Meta;
        if (!_bindings.TryGetValue(entity.GetType(), out var bind))
            return;
        var key = (meta.Topic, meta.Partition);
        lock (_lock)
        {
            var committed = _committed.TryGetValue(key, out var c) ? c : -1;
            if (meta.Offset <= committed)
            {
                _logger?.LogDebug("ManualCommit skip: already committed topic={Topic} p={Partition} off<={Offset}", meta.Topic, meta.Partition, committed);
                Cleanup(key, committed);
                _meta.Remove(entity);
                return;
            }
            var tpo = new TopicPartitionOffset(meta.Topic, new Partition(meta.Partition), new Offset(meta.Offset + 1));
            try
            {
                bind.Commit(tpo);
                _logger?.LogInformation("ManualCommit commit sent: topic={Topic} p={Partition} nextOff={Offset}", meta.Topic, meta.Partition, meta.Offset + 1);
                try
                {
                    // Best-effort: observe committed offset from the consumer for diagnostics
                    var consumer = bind.Consumer;
                    var committedMi = consumer.GetType().GetMethod("Committed", new[] { typeof(IEnumerable<TopicPartition>), typeof(TimeSpan) });
                    if (committedMi != null)
                    {
                        var tps = new[] { new TopicPartition(meta.Topic, new Partition(meta.Partition)) };
                        var res = committedMi.Invoke(consumer, new object[] { tps, TimeSpan.FromSeconds(2) });
                        // Try to read first offset via reflection
                        var enumerable = res as System.Collections.IEnumerable;
                        long? committedValue = null;
                        if (enumerable != null)
                        {
                            foreach (var item in enumerable)
                            {
                                var offProp = item.GetType().GetProperty("Offset");
                                var offVal = offProp?.GetValue(item);
                                if (offVal is Offset off)
                                {
                                    committedValue = off.Value;
                                    break;
                                }
                                else if (offVal != null)
                                {
                                    // fallback for older client types
                                    committedValue = (long)(offVal.GetType().GetProperty("Value")?.GetValue(offVal) ?? -1L);
                                    break;
                                }
                            }
                        }
                        _logger?.LogInformation("ManualCommit observed committed: topic={Topic} p={Partition} committedOff={Committed}", meta.Topic, meta.Partition, committedValue);
                        Console.WriteLine($"[ManualCommit][observed] topic={meta.Topic} p={meta.Partition} committed={committedValue}");
                    }
                }
                catch (System.Exception obsEx)
                {
                    _logger?.LogDebug(obsEx, "ManualCommit committed observation failed");
                }
            }
            catch (System.Exception ex)
            {
                _logger?.LogError(ex, "ManualCommit commit failed: topic={Topic} p={Partition} nextOff={Offset}", meta.Topic, meta.Partition, meta.Offset + 1);
                throw;
            }
            _committed[key] = meta.Offset;
            Cleanup(key, meta.Offset);
            _meta.Remove(entity);
        }
    }

    private void Cleanup((string Topic, int Partition) key, long upto)
    {
        if (!_index.TryGetValue(key, out var dict))
            return;
        var remove = new List<long>();
        foreach (var kv in dict)
        {
            if (kv.Key <= upto || !kv.Value.TryGetTarget(out var target))
            {
                if (kv.Value.TryGetTarget(out var ent))
                    _meta.Remove(ent);
                remove.Add(kv.Key);
            }
        }
        foreach (var k in remove)
            dict.Remove(k);
        if (dict.Count == 0)
            _index.Remove(key);
    }
}
