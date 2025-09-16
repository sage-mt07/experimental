# 足生成 DSL ガイド（日本語整理版）

このドキュメントは「何ができるか」→「どう動くか」→「何に注意するか」の順で、足生成 DSL の全体像をわかりやすく説明します。

できること
- Tick（レートやトレード）から、秒/分/時間/日/週/月の足を生成できる
- 1 つのクエリで複数のタイムフレーム（例: 1m/5m/1h/1d）をまとめて宣言できる
- MarketSchedule（営業日カレンダー）で日/週の境界を安定させられる
- Table は RocksDB にマテリアライズされ、`ToListAsync()` で素早く参照できる

---

## 1. 全体像（まずここだけ読む）

処理フロー（上から下へ）
- 入力: 取引時間外を除いたストリーム（例: `<raw>_filtered`）
- スケジュール結合: `TimeFrame<MarketSchedule>(…, dayKey: …)` で「取引時間内だけ」を選び、日/週の境界を固定
- 窓生成: `Tumbling(r => r.Timestamp, Windows{…}, grace: …)` で複数足を一括生成
- 集計: `GroupBy(...).Select(...)` に書いた集計（例: OHLC）が、そのまま仕様になる
- 欠損埋め（任意）: 連続化が必要な場合だけ `WhenEmpty` を書く
- 出力: 実行側プロファイルで live/final の物理化・命名を決める（DSL には出ない）

要点（前提）
- すべての上位足は 1s_final からフラットに派生します（5m→15m の多段は使用しません）。
- grace は「親 + 1 秒」で段階的に増やします（遅延到着を確実に取り込みます）。
- Table は Streamiz により RocksDB へマテリアライズされ、`ToListAsync()` で参照できます。

最小の書き方（順番：正）
- From → TimeFrame → Tumbling → GroupBy → Select →（必要なら）WhenEmpty

補足（順番の根拠）
- TimeFrame() は「スケジュールでの絞り込み/境界確定」を行い、その後 Tumbling() で窓を張る。
- Tumbling() が窓境界（WindowStart）を定義し、GroupBy()/Select() で OHLC 等の集計を定める。

ポイント
- From: 入力ストリーム（例: DedupRateRecord）を指定
- TimeFrame: 営業時間の拘束が必要なときだけ。日足以上は `dayKey` を付ける
- Tumbling: minutes/hours/days/months をまとめて指定できる
- GroupBy: 主キー（例: Broker, Symbol）
- Select: 集計仕様そのもの（ここに書いた内容が真実）
- WhenEmpty: 欠損埋めをしたいときだけ書く
  - 注意: WhenEmpty/Prev/Fill の連携には Select 内で WindowStart() を1回含めること（バケット列が必須）

``` mermaid

flowchart TB
  %% ============ 上流 ============
  subgraph Upstream["上流（取引時間外除外）"]
    raw["<raw>"]
    filtered["<raw>_filtered\nLINQ: Where(...) 等で取引時間外を除外"]
    raw --> filtered
  end

  %% ============ DSL ============
  subgraph DSL["C# アプリケーション / DSL (LINQ式ツリー)"]
    TF["TimeFrame<MarketSchedule>\nLINQ: Join/Where(dayKey: MarketDate)"]
    Tumble["Tumbling\nLINQ: Window式（複数足まとめて生成）"]
    GroupBy["GroupBy(主キー)"]
    Select["Select(OHLC 等の仕様)\nLINQ: EarliestByOffset/Max/Min/LatestByOffset"]
  end
  filtered --> TF --> Tumble --> GroupBy --> Select

  %% ============ WhenEmpty（HB/Prev合流） ============
  subgraph Fill["欠損埋めフロー（WhenEmpty 記述時のみ）"]
    HB["HB: HeartBeat\n(Tumbling が次の WindowStart を提示)"]
    Prev["Prev: 直近の確定レコード"]
    Join["LEFT JOIN (HB × base series)\n不足バケット検出"]
    Apply["WhenEmpty(prev, next)\n→ next を埋めて確定"]
  end
  Select -->|base series| Join
  HB -.->|WindowStart 提示| Join
  Prev -.->|前バケット値| Apply
  Join --> Apply

  %% ============ 1s_final ハブ ============
  subgraph Hub["確定 1 秒足ハブ"]
    final1s["bar_1s_final (TABLE)"]
    final1s_s["bar_1s_final_s (STREAM)\n※上位足の唯一の親入力"]
    final1s --> final1s_s
  end
  Apply -->|DDL/CSAS/CTAS| final1s

  %% ============ 上位足（flat派生） ============
  subgraph Live["上位足 (live系: EMIT CHANGES)"]
    m1["bar_1m_live"]
    m5["bar_5m_live"]
    m15["bar_15m_live"]
    h1["bar_1h_live"]
    d1["bar_1d_live"]
    w1["bar_1w_live"]
  end
  final1s_s --> m1
  final1s_s --> m5
  final1s_s --> m15
  final1s_s --> h1
  final1s_s --> d1
  final1s_s --> w1

  %% ============ ローカルキャッシュ / 読み取り ============
  subgraph Cache["ローカルキャッシュ / 読み取り"]
    streamiz["Streamiz"]
    rocks["RocksDB 状態ストア"]
    timebucket["LINQ: TimeBucket(from,to[,keyPrefix])\n（時間範囲で取得／前方一致キーにも対応）\nctx.TimeBucket からも取得可能"]
    streamiz --> rocks --> timebucket
  end

  %% 並行するストリーム購読
  subgraph StreamRead["ストリーム購読（ライブ）"]
    pushpull["LINQ: ForEachAsync()/Push/Pull"]
  end

  %% live 出力→利用面へ
  m1 --> streamiz
  m5 --> streamiz
  m15 --> streamiz
  h1 --> streamiz
  d1 --> streamiz
  w1 --> streamiz

  m1 --> pushpull
  m5 --> pushpull
  m15 --> pushpull
  h1 --> pushpull
  d1 --> pushpull
  w1 --> pushpull

  %% ============ スタイル定義 ============
  %% 色：緑=入力, 紫=DSL/変換, 青=DB/ストリーム, オレンジ=出力, 黄=WhenEmpty補助
  classDef in fill:#e9f7ef,stroke:#27ae60,color:#145a32;
  classDef dsl fill:#efe9fb,stroke:#8e44ad,color:#4a235a;
  classDef gen fill:#efe9fb,stroke:#8e44ad,color:#4a235a;
  classDef db fill:#eaf2fb,stroke:#2980b9,color



```

### 1.1 1秒足最終TABLEと上位足DDL（UT確認用）

- 1秒最終TABLEでは `WINDOWSTART` を `BucketStart` として投影する（投影で1回必須）。
- GROUP BY への `WINDOWSTART` の明記は任意（推奨）。投影に含めれば生成系は正しく動作する。
- 上位の1分・5分は、1s TABLE のチェンジログを素の STREAM（`*_1s_final_s`）として読み、同様に投影で時刻列を含める。
- CREATE TABLE/STREAM の WITH 句は `KEY_FORMAT='AVRO'` と `VALUE_FORMAT='AVRO'` を明示する。
- CREATE TABLE 時は Schema Registry に登録した `VALUE_AVRO_SCHEMA_FULL_NAME` を WITH 句に設定する。
- DECIMAL の精度はアプリ依存（既定: 18,2）。必要に応じてプロパティ単位で上書きする。

```sql
-- 1) 1s 最終 TABLE（集計＆正規化）
CREATE TABLE BAR_1S_FINAL WITH (
  KAFKA_TOPIC='bar_1s_final',   -- 明示推奨
  KEY_FORMAT='AVRO',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='kafka_ksql_linq_bars.bar_1s_final_valueAvro', -- SR登録名と一致させる
  PARTITIONS=3,                 -- 想定スループットで調整
  REPLICAS=1
) AS
SELECT
  Broker,
  Symbol,
  WINDOWSTART AS BucketStart,
  MIN(Bid) AS Low,
  MAX(Bid) AS High,
  LATEST_BY_OFFSET(FirstBid) AS Open,
  LATEST_BY_OFFSET(LastBid)  AS Close
FROM TICKS
WINDOW TUMBLING (SIZE 1 SECOND)
GROUP BY Broker, Symbol, WINDOWSTART;

-- 2) TABLE の変更ログに素でかぶせた中間 STREAM
CREATE STREAM BAR_1S_FINAL_S (
  Broker STRING KEY,
  Symbol STRING KEY,
  BucketStart TIMESTAMP,
  Open DECIMAL(18,6),
  High DECIMAL(18,6),
  Low  DECIMAL(18,6),
  Close DECIMAL(18,6)
) WITH (
  KAFKA_TOPIC='bar_1s_final',
  KEY_FORMAT='AVRO',
  VALUE_FORMAT='AVRO'
);

-- 3) 下流（例：1m/5m）
CREATE TABLE BAR_1M_LIVE WITH (
  KEY_FORMAT='AVRO',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='kafka_ksql_linq_bars.bar_1m_live_valueAvro' -- SR登録名と一致させる
) AS
SELECT
  Broker,
  Symbol,
  WINDOWSTART AS BucketStart,
  MIN(Low)  AS Low,
  MAX(High) AS High,
  EARLIEST_BY_OFFSET(Open) AS Open,
  LATEST_BY_OFFSET(Close)  AS Close
FROM BAR_1S_FINAL_S
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY Broker, Symbol, WINDOWSTART;

CREATE TABLE BAR_5M_LIVE WITH (
  KEY_FORMAT='AVRO',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='kafka_ksql_linq_bars.bar_5m_live_valueAvro' -- SR登録名と一致させる
) AS
SELECT
  Broker,
  Symbol,
  WINDOWSTART AS BucketStart,
  MIN(Low)  AS Low,
  MAX(High) AS High,
  EARLIEST_BY_OFFSET(Open) AS Open,
  LATEST_BY_OFFSET(Close)  AS Close
FROM BAR_1S_FINAL_S
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY Broker, Symbol, WINDOWSTART;
```


## 2. 処理の詳細（ここから深掘り）


### 2.1 TimeFrame と dayKey（営業日の境界）
```csharp
.TimeFrame<MarketSchedule>((r, s) =>
       r.Broker == s.Broker
    && r.Symbol == s.Symbol
    && s.Open <= r.Timestamp && r.Timestamp < s.Close,
    dayKey: s => s.MarketDate)
```
運用のコツ
- スケジュール判定は上流で実施します（例: `<raw>_filtered` を作成して参照します）。
- `dayKey` は「日/週/月などの境界を安定させる」ためのマーカーです。
- 分/時間足では原則不要です（指定しても構いません）。

### 2.2 TimeFrame と Tumbling（複数足をまとめて宣言）
```csharp
q.From<DedupRateRecord>()
 .TimeFrame<MarketSchedule>((r, s) =>
        r.Broker == s.Broker
     && r.Symbol == s.Symbol
     && s.OpenTime <= r.Ts && r.Ts < s.CloseTime)
 .Tumbling(r => r.Ts,
     new Windows {
         Minutes = new[]{ 5, 15, 30 },
         Hours   = new[]{ 1, 4, 8 },
         Days    = new[]{ 1, 7 },
         Months  = new[]{ 1, 12 }
     },
     grace: TimeSpan.FromMinutes(2))
```
使いどころ
- 1 回の宣言で複数の足をまとめて指定できます。
- grace は実行側の解釈に委ねます（内部では「親 + 1 秒」で伝播します）。
- 中間足や BaseUnit は非公開です（利用者が意識する必要はありません）。

### 2.3 GroupBy（主キー）
```csharp
.GroupBy(r => new { r.Broker, r.Symbol })
```
主キーの考え方
- GroupBy キー + バケット列（WindowStart）が主キーになります。

### 2.4 GroupBy と Select（投影＝仕様）
```csharp
q.From<DedupRateRecord>()
 .TimeFrame<MarketSchedule>((r, s) => r.Broker == s.Broker && r.Symbol == s.Symbol && s.OpenTime <= r.Ts && r.Ts < s.CloseTime)
 .Tumbling(r => r.Ts, new Windows { Minutes = new[]{ 1 } })
 .GroupBy(r => new { r.Broker, r.Symbol })
 .Select(g => new OneMinuteCandle {
     Broker   = g.Key.Broker,
     Symbol   = g.Key.Symbol,
     BarStart = g.WindowStart(),            // ← バケット列（“式”で認識、列名は任意）
     Open  = g.EarliestByOffset(x => x.Bid),
     High  = g.Max(x => x.Bid),
     Low   = g.Min(x => x.Bid),
     Close = g.LatestByOffset(x => x.Bid)
 })
```
作るときの注意
- `g.WindowStart()` を必ず 1 回投影してください（列名は任意、式で識別します）。
- OHLC などの定義はアプリ側で明示してください（固定ではありません）。
- 派生段の投影は SELECT *（恒等）です。列名の固定や属性依存は行いません。

### 2.5 WhenEmpty（必要なときだけ・欠損埋め）
```csharp
.WhenEmpty((previous, next) =>
{
    next.Broker = previous.Broker;
    next.Symbol = previous.Symbol;
    next.Open   = previous.Close;
    next.High   = previous.Close;
    next.Low    = previous.Close;
    next.Close  = previous.Close;
    return next;
})
```
ポイント
- WhenEmpty を記述したときだけ「連続化モード」になります（HB + LEFT JOIN + Fill）。
- 記述しなければ疎のままです（デンス化しません）。
- 欠損埋めの結果を上流（final）へ戻さないでください（循環禁止）。

### 2.6 Table キャッシュと ToListAsync（RocksDB）
- Table は Streamiz により RocksDB にマテリアライズされます（StateStore）。
- `ToListAsync()` は「RUNNING 待ち → ストア全件列挙」を実行します。
- 前方一致フィルタは「NUL 区切りの文字列キー」で実現します。
- 伝達時間の目安は、通常 50〜200ms、起動直後は 0.5〜3 秒です（環境依存）。
- Stream ソースは `ToListAsync()` 非対応です（Push 購読を使用します）。
- Consumer→RocksDB 連携では `MappingRegistry.GetMapping()` が返す `KeyValueTypeMapping` を使って `SchemaAvroSerDes` を構成し、`AvroValueSchema` に保持された `VALUE_AVRO_SCHEMA_FULL_NAME` をそのまま再利用します。これにより DDL と同じ値スキーマ名のまま RocksDB を読み書きできます。【F:src/Cache/Extensions/KsqlContextCacheExtensions.cs†L39-L74】【F:src/Mapping/KeyValueTypeMapping.cs†L15-L36】

---

## 3. 内部の前提（知っておくと安心）
- 1s ハブ（= 1s_final）からフラットに派生します（5m→15m の多段は禁止です）。
- BaseUnitSeconds は 60 の約数のみ有効です（内部で自動展開します）。
- WindowStart は式で識別します（列名には依存しません）。
- 実行モードや物理名はプロファイル側で決定します（DSL では非公開です）。
- 欠損埋めの循環は禁止です（下流→上流へ戻しません）。
- grace は「親 + 1 秒」で階段的に伝播します。


---

## 4. バリデーション（自動チェック）
- BaseUnitSeconds は 60 の約数
- ウィンドウは BaseUnitSeconds の倍数（1m 以上は分の整数倍）
- grace は「親+1秒」を満たす
- よくあるエラー
  - Base unit must divide 60 seconds.
  - Windows ≥ 1 minute must be whole-minute multiples.
  - Windowed query requires exactly one WindowStart() in projection.

---

## 5. 代表シナリオ（複数足を一括生成）
- 秒/分/時間/日/月を一括宣言（1s_final ハブに一本化）
- 欠損埋めが必要な時だけ WhenEmpty を付ける

---

## 6. 1m→5m ロールアップ（設計/検証）

### 6.1 設計（同一ソースから 1m/5m をフラット派生）
実装は「From → TimeFrame（任意）→ Tumbling → GroupBy → Select」。複数足は Windows でまとめて宣言します。

```csharp
// 例: DedupRateRecord (Ts, Broker, Symbol, Bid)
b.Entity<Candle1m>().ToQuery(q => q
    .From<DedupRateRecord>()
    .Tumbling(r => r.Ts, new Windows { Minutes = new[] { 1 } })
    .GroupBy(r => new { r.Broker, r.Symbol })
    .Select(g => new Candle1m {
        Broker   = g.Key.Broker,
        Symbol   = g.Key.Symbol,
        BarStart = g.WindowStart(),
        Open  = g.EarliestByOffset(x => x.Bid),
        High  = g.Max(x => x.Bid),
        Low   = g.Min(x => x.Bid),
        Close = g.LatestByOffset(x => x.Bid)
    }));

b.Entity<Candle5m>().ToQuery(q => q
    .From<DedupRateRecord>()
    .Tumbling(r => r.Ts, new Windows { Minutes = new[] { 5 } })
    .GroupBy(r => new { r.Broker, r.Symbol })
    .Select(g => new Candle5m {
        Broker   = g.Key.Broker,
        Symbol   = g.Key.Symbol,
        BarStart = g.WindowStart(),
        Open  = g.EarliestByOffset(x => x.Bid),
        High  = g.Max(x => x.Bid),
        Low   = g.Min(x => x.Bid),
        Close = g.LatestByOffset(x => x.Bid)
    }));
```

ポイント
- 1m/5m は 1s_final からフラットに派生（多段ロールアップは行わない）
- grace は親 + 1 秒で自動伝播（詳細は 2.2）

### 6.2 検証（1m の集約結果と 5m の一致を確認）
アプリ側で 1m を 5m に再集約し、OHLC の一致をチェックします。

```csharp
// 前提: ctx.Set<Candle1m>().ToListAsync(), ctx.Set<Candle5m>().ToListAsync() で
//       同一期間・同一銘柄の 1m/5m を取得済み

static DateTime FloorTo5Min(DateTime dt)
{
    var ticks5m = TimeSpan.FromMinutes(5).Ticks;
    return new DateTime((dt.Ticks / ticks5m) * ticks5m, DateTimeKind.Utc);
}

var grouped1m = oneMin
    .GroupBy(c => FloorTo5Min(c.BarStart))
    .ToDictionary(g => g.Key, g => new {
        Open  = g.OrderBy(x => x.BarStart).First().Open,
        High  = g.Max(x => x.High),
        Low   = g.Min(x => x.Low),
        Close = g.OrderBy(x => x.BarStart).Last().Close
    });

var mismatches = new List<string>();
foreach (var b5 in fiveMin.OrderBy(x => x.BarStart))
{
    if (!grouped1m.TryGetValue(b5.BarStart, out var roll))
    {
        mismatches.Add($"[missing] no 1m group for 5m {b5.BarStart:HH:mm}");
        continue;
    }
    bool eq(decimal a, decimal b) => a == b; // 設計上は厳密一致
    if (!eq(b5.Open, roll.Open) || !eq(b5.High, roll.High) || !eq(b5.Low, roll.Low) || !eq(b5.Close, roll.Close))
    {
        mismatches.Add($"[mismatch] 5m {b5.BarStart:HH:mm} O:{b5.Open}/{roll.Open} H:{b5.High}/{roll.High} L:{b5.Low}/{roll.Low} C:{b5.Close}/{roll.Close}");
    }
}

if (mismatches.Count == 0)
    Console.WriteLine("[ok] 5m equals rollup from 1m");
else
    foreach (var m in mismatches) Console.WriteLine(m);
```

TimeBucket を使った取得（ctx 経由）

```csharp
// KsqlContext ctx; Broker/Symbol は主キー
var one = await ctx.TimeBucket.Get<Bar>(Period.Minutes(1))
    .ToListAsync(new[]{ broker, symbol }, CancellationToken.None);
var five = await ctx.TimeBucket.Get<Bar>(Period.Minutes(5))
    .ToListAsync(new[]{ broker, symbol }, CancellationToken.None);
```

補足
- 上記の検証は examples/rollup-1m-5m-verify に近い内容です。
- 実際の検証では取引時間の拘束や WhenEmpty による補完有無を加味してください。

---

## 6. 拡張ポイント
- Aggregation Policy（例: VWAP, Volume, Trades）
- MarketSchedule（dayKey = MarketDate など）
- 命名/物理化は実行プロファイルで管理（DSL には出さない）

---

## 7. テストの観点（サクッと）
- `WindowStart()` が1回だけ含まれるか
- バリデーション（BaseUnit、倍数、分単位、循環検出）
- 合成ロジックの一貫性（1m→上位）
- 日足以上は dayKey の境界そろえ

---

## 8. 禁則（NG 集）
- `.EmitChanges()` / `.AsFinal()` など内部モードを匂わせない
- `.ToSink("…")` など物理名を DSL に露出しない
- 5m→15m の多段ロールアップは禁止（常に 1s_final から）
- 確定系列に Hopping を混在させない（速報系は別DAGに）

## 9. 命名規約（覚えどころ）

- **テーブル/トピック名**: `<entity>_<timeframe>_(live|final)`
  - 例: `bar_1s_final`, `bar_1m_live`, `bar_5m_live`, `bar_1d_live`
  - timeframe: `s`=秒, `m`=分, `h`=時間, `d`=日, `mo`=月
  - live/final: 集計モードの明示
  - filteredraw/nontrading_raw: `<raw_stream>_filtered` を参照（生成は上流責務）
- 1s_final は全上位足の唯一の親

1s_final / 1s_final_s（役割）
- 1s_final: EMIT FINAL の 1 秒確定足（TABLE）
- 1s_final_s: 1s_final を STREAM 化した入力専用の親
- ルール: 上位足は常に `<entity>_1s_final_s` を入力にする

---

## 10. 付録: 最小サンプル（コピペで雰囲気を掴む）
```csharp
EventSet<Rate>()
  .From<DeDupRates>()
  .ToQuery(q => q
    .TimeFrame<MarketSchedule>((r, s) =>
           r.Broker == s.Broker
        && r.Symbol == s.Symbol
        && s.Open <= r.Timestamp && r.Timestamp < s.Close,
        dayKey: s => s.MarketDate)

    .Tumbling(r => r.Timestamp, new Windows {
        Minutes = new[]{ 5, 15, 30 },
        Hours   = new[]{ 1, 4, 8 },
        Days    = new[]{ 1, 7 },
        Months  = new[]{ 1, 12 }
    }, grace: TimeSpan.FromMinutes(2))

    .GroupBy(r => new { r.Broker, r.Symbol })

    .Select(g => new {
        g.Key.Broker,
        g.Key.Symbol,
        g.WindowStart(),
        Open  = g.EarliestByOffset(x => x.Bid),
        High  = g.Max(x => x.Bid),
        Low   = g.Min(x => x.Bid),
        Close = g.LatestByOffset(x => x.Bid)
    })

    //.WhenEmpty((prev, next) => { /* 任意で欠損埋め */ return next; })
  );
```

> 実行モード（live/final）や命名/物理化は実行プロファイルで決定（DSL には出さない）
