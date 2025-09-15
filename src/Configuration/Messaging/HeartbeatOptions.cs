using System;
using System.ComponentModel;

namespace Kafka.Ksql.Linq.Configuration.Messaging;

public class LeaderElectionOptions
{
    [DefaultValue("")]
    public string GroupId { get; init; } = string.Empty;

    [DefaultValue("")]
    public string InstanceId { get; init; } = string.Empty;
}

public class HeartbeatOptions
{
    [DefaultValue("hb_1m")]
    public string Topic { get; init; } = "hb_1m";

    [DefaultValue(typeof(LeaderElectionOptions))]
    public LeaderElectionOptions LeaderElection { get; init; } = new();

    /// <summary>
    /// Additional delay after each bucket before emitting a heartbeat.
    /// </summary>
    public TimeSpan Grace { get; init; } = TimeSpan.Zero;
}
