# OnModelCreating Sample Catalog

This page lists minimal `modelBuilder.Entity<T>().ToQuery(...)` examples for defining ksqlDB queries. It focuses on patterns you will use frequently in production.

## 1. Simple filter + projection (works for Pull or Push)
```csharp
modelBuilder.Entity<ActiveOrder>().ToQuery(q => q
    .From<Order>()
    .Where(o => o.IsActive)
    .Select(o => new ActiveOrder { Id = o.Id, Amount = o.Amount }));
```

## 2. Stream-stream JOIN (WITHIN required)
```csharp
modelBuilder.Entity<OrderPayment>().ToQuery(q => q
    .From<Order>()
    .Join<Payment>((o, p) => o.Id == p.OrderId)
    .Within(TimeSpan.FromMinutes(5))
    .Select((o, p) => new OrderPayment { OrderId = o.Id, Paid = p.Paid }));
```

## 3. GroupBy + aggregation (Push delivery)
```csharp
modelBuilder.Entity<OrderStats>().ToQuery(q => q
    .From<Order>()
    .GroupBy(o => o.CustomerId)
    .Select(g => new OrderStats
    {
        CustomerId = g.Key,
        Total = g.Sum(x => x.Amount)
    })
    // EMIT CHANGES is inferred automatically (Push)
    );
```

## 4. HAVING clause for thresholds
```csharp
modelBuilder.Entity<BigCustomer>().ToQuery(q => q
    .From<Order>()
    .GroupBy(o => o.CustomerId)
    .Having(g => g.Sum(x => x.Amount) > 1000)
    .Select(g => new BigCustomer { CustomerId = g.Key, Total = g.Sum(x => x.Amount) }));
```

## 5. CASE expression (labeling conditions)
```csharp
modelBuilder.Entity<CustomerStatus>().ToQuery(q => q
    .From<Order>()
    .GroupBy(o => o.CustomerId)
    .Select(g => new CustomerStatus
    {
        CustomerId = g.Key,
        Status = g.Sum(x => x.Amount) > 1000 ? "VIP" : "Regular"
    }));
```

## 6. Fix DECIMAL precision (attribute)
```csharp
public class Order
{
    public int CustomerId { get; set; }
    [KsqlDecimal(18, 2)]
    public decimal Amount { get; set; }
}

modelBuilder.Entity<OrderTotal>().ToQuery(q => q
    .From<Order>()
    .GroupBy(o => o.CustomerId)
    .Select(g => new OrderTotal { CustomerId = g.Key, Total = g.Sum(x => x.Amount) }));
```

## 7. Time window (1-minute tumbling, Push)
```csharp
modelBuilder.Entity<MinuteSales>().ToQuery(q => q
    .From<Order>()
    .Tumbling(o => o.OrderTime, TimeSpan.FromMinutes(1))
    .GroupBy(o => o.CustomerId)
    .Select(g => new MinuteSales
    {
        CustomerId = g.Key,
        BucketStart = g.WindowStart(),
        Total = g.Sum(x => x.Amount)
    })
    // EMIT CHANGES is inferred automatically (Push)
    );
```

---

## Success checklist
- Always add `.Within(...)` to JOINs.
- Resolve mixed aggregate/non-aggregate columns with `GROUP BY` (the builder validates this).
- Declare decimal precision with `[KsqlDecimal(p, s)]` to align schema and application.
- Push/Pull-specific methods were removed. Aggregations (such as `GroupBy`) infer Push and add `EMIT CHANGES`, while TABLE projections remain Pull (no `EMIT CHANGES`).
