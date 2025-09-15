# OnModelCreating サンプル集

`modelBuilder.Entity<T>().ToQuery(...)` を使った ksqlDB クエリ定義の最小サンプルです。実務でよく使う形だけを厳選して記載します。

## 1. 単純フィルタ＋投影（Pull/Pushどちらでも）
```csharp
modelBuilder.Entity<ActiveOrder>().ToQuery(q => q
    .From<Order>()
    .Where(o => o.IsActive)
    .Select(o => new ActiveOrder { Id = o.Id, Amount = o.Amount }));
```

## 2. 2ストリームJOIN（WITHIN 必須）
```csharp
modelBuilder.Entity<OrderPayment>().ToQuery(q => q
    .From<Order>()
    .Join<Payment>((o, p) => o.Id == p.OrderId)
    .Within(TimeSpan.FromMinutes(5))
    .Select((o, p) => new OrderPayment { OrderId = o.Id, Paid = p.Paid }));
```

## 3. GroupBy＋集計（Push配信）
```csharp
modelBuilder.Entity<OrderStats>().ToQuery(q => q
    .From<Order>()
    .GroupBy(o => o.CustomerId)
    .Select(g => new OrderStats
    {
        CustomerId = g.Key,
        Total = g.Sum(x => x.Amount)
    })
    // EMIT CHANGES は自動付与（Push 推論）
    );
```

## 4. HAVING 句で閾値を絞る
```csharp
modelBuilder.Entity<BigCustomer>().ToQuery(q => q
    .From<Order>()
    .GroupBy(o => o.CustomerId)
    .Having(g => g.Sum(x => x.Amount) > 1000)
    .Select(g => new BigCustomer { CustomerId = g.Key, Total = g.Sum(x => x.Amount) }));
```

## 5. CASE（条件ラベル付け）
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

## 6. DECIMAL 精度の固定（属性）
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

## 7. 時間窓（TUMBLING 1分、Push）
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
    // EMIT CHANGES は自動付与（Push 推論）
    );
```

---

成功確認チェックリスト

- JOIN は `.Within(...)` を必ず付ける
- 集計と非集計の混在は GROUP BY で解消（混在エラーはビルダーが検知）
- DECIMAL は `[KsqlDecimal(p,s)]` で明示し、アプリとスキーマの精度を一致
- Push/Pull の指定メソッドは廃止。GroupBy等は Push として自動的に `EMIT CHANGES` が付与され、TABLE系は Pull（`EMIT CHANGES` なし）。
