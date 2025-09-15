using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Query.Analysis;

internal static class DerivationPlanner
{
    public static IReadOnlyList<DerivedEntity> Plan(TumblingQao qao, EntityModel model, bool whenEmpty = false)
    {
        var entities = new List<DerivedEntity>();

        var keyShapes = qao.Keys.Select(k =>
        {
            var match = qao.PocoShape.FirstOrDefault(p => p.Name == k)
                ?? throw new InvalidOperationException($"Key property '{k}' not found");
            return match;
        }).ToArray();
        var valueShapes = qao.PocoShape.ToArray();

        var basedOn = qao.BasedOn;

        var topicAttr = model.EntityType.GetCustomAttribute<KsqlTopicAttribute>();
        var baseId = (topicAttr?.Name ?? model.TopicName ?? model.EntityType.Name).ToLowerInvariant();
        var windows = qao.Windows
            .OrderBy(w => w.Unit switch
            {
                "s" => w.Value / 60m,
                "m" => w.Value,
                "h" => w.Value * 60,
                "d" => w.Value * 1440,
                "wk" => w.Value * 10080,
                "mo" => w.Value * 43200m,
                _ => w.Value
            })
            .ToList();
        if (!windows.Any(w => w.Unit == "s" && w.Value == 1))
            windows.Insert(0, new Timeframe(1, "s"));
        var graceMap = new Dictionary<string, int>();
        var prevGrace = qao.GraceSeconds ?? 0;
        foreach (var tf in windows)
        {
            var key = $"{tf.Value}{tf.Unit}";
            if (qao.GracePerTimeframe.TryGetValue(key, out var parent))
                prevGrace = parent;
            var next = prevGrace + 1;
            graceMap[key] = next;
            prevGrace = next;
        }
        qao.GracePerTimeframe.Clear();
        foreach (var kv in graceMap)
            qao.GracePerTimeframe[kv.Key] = kv.Value;
        // HB は WhenEmpty 指定時のみ有効化
        var enableHb = whenEmpty;
        var hub = $"{baseId}_1s_final_s";
        foreach (var tf in windows)
        {
            var tfStr = $"{tf.Value}{tf.Unit}";
            var hbId = $"{baseId}_hb_{tfStr}";
            if (tf.Unit == "s" && tf.Value == 1)
            {
                // ksqlDB requires the backing topic to exist; create TABLE first, then STREAM referencing it.
                var final1s = new DerivedEntity
                {
                    Id = $"{baseId}_1s_final",
                    Role = Role.Final1s,
                    Timeframe = tf,
                    KeyShape = keyShapes,
                    ValueShape = valueShapes,
                    // 1s TABLE is derived directly from the original source (no hub dependency)
                    InputHint = null,
                    BasedOnSpec = basedOn,
                    WeekAnchor = qao.WeekAnchor,
                    GraceSeconds = graceMap[tfStr]
                };
                entities.Add(final1s);

                var final1sStream = new DerivedEntity
                {
                    Id = hub,
                    Role = Role.Final1sStream,
                    Timeframe = tf,
                    KeyShape = keyShapes,
                    ValueShape = valueShapes,
                    // 1s STREAM reads from the 1s TABLE
                    InputHint = final1s.Id,
                    BasedOnSpec = basedOn,
                    WeekAnchor = qao.WeekAnchor,
                    GraceSeconds = graceMap[tfStr]
                };
                entities.Add(final1sStream);

                if (enableHb)
                {
                    var hb1s = new DerivedEntity
                    {
                        Id = hbId,
                        Role = Role.Hb,
                        Timeframe = tf,
                        KeyShape = keyShapes,
                        ValueShape = Array.Empty<ColumnShape>(),
                        InputHint = hub,
                        BasedOnSpec = basedOn,
                        WeekAnchor = qao.WeekAnchor,
                        // HB は各足の grace を含めて終端を駆動
                        GraceSeconds = graceMap[tfStr]
                    };
                    entities.Add(hb1s);
                }
                continue;
            }

            var liveId = $"{baseId}_{tfStr}_live";
            var live = new DerivedEntity
            {
                Id = liveId,
                Role = Role.Live,
                Timeframe = tf,
                KeyShape = keyShapes,
                ValueShape = valueShapes,
                // Live frames aggregate from the 1s hub stream
                InputHint = hub,
                BasedOnSpec = basedOn,
                WeekAnchor = qao.WeekAnchor,
                GraceSeconds = graceMap[tfStr]
            };
            entities.Add(live);

            // WhenEmpty のときのみ、各足に対して HB を1つ生成する
            if (enableHb)
            {
                var hb = new DerivedEntity
                {
                    Id = hbId,
                    Role = Role.Hb,
                    Timeframe = tf,
                    KeyShape = keyShapes,
                    ValueShape = Array.Empty<ColumnShape>(),
                    InputHint = hub,
                    BasedOnSpec = basedOn,
                    WeekAnchor = qao.WeekAnchor,
                    GraceSeconds = graceMap[tfStr]
                };
                entities.Add(hb);
            }

            if (whenEmpty)
            {
                var fill = new DerivedEntity
                {
                    Id = $"{baseId}_{tfStr}_fill",
                    Role = Role.Fill,
                    Timeframe = tf,
                    KeyShape = keyShapes,
                    ValueShape = valueShapes,
                    InputHint = hub,
                    BasedOnSpec = basedOn,
                    WeekAnchor = qao.WeekAnchor,
                    GraceSeconds = graceMap[tfStr]
                };
                entities.Add(fill);
            }

            // prev_1m is only necessary for WhenEmpty filler path
            if (whenEmpty && tf.Unit == "m" && tf.Value == 1)
            {
                var prev = new DerivedEntity
                {
                    Id = $"{baseId}_prev_1m",
                    Role = Role.Prev1m,
                    Timeframe = tf,
                    KeyShape = keyShapes,
                    ValueShape = valueShapes,
                    InputHint = hub,
                    BasedOnSpec = basedOn,
                    WeekAnchor = qao.WeekAnchor,
                    GraceSeconds = graceMap[tfStr]
                };
                entities.Add(prev);
            }
        }
        return entities;
    }
}
