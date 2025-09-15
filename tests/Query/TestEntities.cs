using System;

#nullable enable

using Kafka.Ksql.Linq.Core.Attributes;

namespace Kafka.Ksql.Linq.Tests;

[KsqlTopic("test-topic")]
public class TestEntity
{
    [KsqlKey(Order = 0)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool IsActive { get; set; }
    public bool? IsProcessed { get; set; }
    public string Type { get; set; } = string.Empty;
}

public class ChildEntity
{
    public int Id { get; set; }
    public int ParentId { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class GrandChildEntity
{
    public int Id { get; set; }
    public int ChildId { get; set; }
    public string Description { get; set; } = string.Empty;
}

public class Order
{
    public int OrderId { get; set; }
    public int CustomerId { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal TotalAmount { get; set; }
}

public class Customer
{
    public int Id { get; set; }
    public string? Region { get; set; }
}

public class Payment
{
    public int OrderId { get; set; }
    public string Status { get; set; } = string.Empty;
}
