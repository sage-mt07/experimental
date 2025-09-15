# KSQL関数とデータ型の対応表（主要関数）

このガイドは SQL Server → ksqlDB の型・関数対応の早見表です。

## データ型の対応

| SQL Server | ksqlDB/Avro | 注記 |
|---|---|---|
| TINYINT/SMALLINT/INT | INTEGER | 範囲に注意（オーバーフロー時は型調整） |
| BIGINT | BIGINT | そのまま |
| BIT | BOOLEAN | 0/1→false/true |
| DECIMAL(p,s), NUMERIC(p,s) | DECIMAL(p,s) | 精度・スケールはスキーマで指定 |
| REAL/FLOAT | DOUBLE | 丸め差に注意 |
| MONEY/SMALLMONEY | DECIMAL(19,4) 相当 | 運用方針でDECIMALへ正規化推奨 |
| CHAR/NCHAR/VARCHAR/NVARCHAR | VARCHAR | 文字コードはUTF-8前提（必要に応じ正規化） |
| BINARY/VARBINARY | BYTES | バイナリは Base64 可視化される場合あり |
| UNIQUEIDENTIFIER | VARCHAR または BYTES | 文字列GUIDで扱うのが簡便 |
| DATE | DATE | そのまま |
| TIME | TIME | そのまま |
| DATETIME/SMALLDATETIME/DATETIME2 | TIMESTAMP | エポックms基準。タイムゾーンに注意 |
| XML/JSON | VARCHAR | JSONは文字列+JSON_* 関数で抽出 |
| GEOGRAPHY/GEOMETRY | VARCHAR/STRUCT | GeoJSON等へ変換して格納推奨 |

## 文字列関数

| SQL Server | ksqlDB | 例 |
|---|---|---|
| UPPER | UPPER | UPPER(Name) |
| LOWER | LOWER | LOWER(Name) |
| SUBSTRING | SUBSTRING | SUBSTRING(Name, 1, 3) |
| LEN | LEN | LEN(Name)（末尾空白の扱い差に留意） |
| TRIM | TRIM | TRIM(Name) |
| REPLACE | REPLACE | REPLACE(Name,'a','b') |
| CHARINDEX | INSTR | INSTR(Name,'foo') |
| CONCAT | CONCAT | CONCAT(A,B,...) |

## 数値・数学関数

| SQL Server | ksqlDB | 例 |
|---|---|---|
| ABS | ABS | ABS(x) |
| ROUND | ROUND | ROUND(x[,d]) |
| FLOOR | FLOOR | FLOOR(x) |
| CEILING | CEIL | CEIL(x) |
| SQRT | SQRT | SQRT(x) |
| POWER | POWER | POWER(x,y) |
| SIGN | SIGN | SIGN(x) |
| LOG/LOG10 | LOG/LOG10 | LOG(x), LOG10(x) |
| EXP | EXP | EXP(x) |

## 日付・時刻

| SQL Server | ksqlDB | 例/注記 |
|---|---|---|
| DATEADD | DATEADD | DATEADD('minute', 5, ts)（AddMinutes に相当） |
| DATEDIFF | なし（計算で代替） | (ts2 - ts1) 差分msの算術処理で代替 |
| YEAR/MONTH/DAY | YEAR/MONTH/DAY | YEAR(ts) 等 |
| HOUR/MINUTE/SECOND | HOUR/MINUTE/SECOND | そのまま |
| GETDATE()/SYSDATETIME() | 非推奨/未サポート | 行時刻は ROWTIME を利用、必要なら事前加工 |
| FORMAT | なし | アプリ側整形推奨 |

## 集計・集合

| SQL Server | ksqlDB | 注記 |
|---|---|---|
| COUNT/SUM/AVG/MIN/MAX | COUNT/SUM/AVG/MIN/MAX | そのまま |
| COUNT(DISTINCT) | COUNT_DISTINCT | パフォーマンス要件に留意 |
| TOP/ROW_NUMBER | TOPK/TOPKDISTINCT | 厳密な順位付けは別途設計 |

## JSON

| SQL Server | ksqlDB | 例 |
|---|---|---|
| JSON_VALUE | JSON_EXTRACT_STRING | JSON_EXTRACT_STRING(col,'$.path') |
| JSON_QUERY | JSON_RECORDS | JSON_RECORDS(col) |
| JSON_ARRAY_LENGTH | JSON_ARRAY_LENGTH | そのまま |
| JSON_KEYS | JSON_KEYS | そのまま |

## 条件/変換

| SQL Server | ksqlDB | 例 |
|---|---|---|
| CASE WHEN | CASE | CASE WHEN cond THEN a ELSE b END |
| COALESCE | COALESCE | COALESCE(a,b,...) |
| ISNULL | IFNULL | IFNULL(a,b) |
| CAST/CONVERT | CAST | CAST(x AS VARCHAR/INTEGER/DOUBLE/DECIMAL(...)) |

## 注意事項（よくあるハマり）

- 改行/空白差: 生成SQLの比較は改行コード差（CRLF/LF）を吸収すること。
- 小数精度: DECIMAL(p,s) はスキーマで固定。アプリとドキュメントの整合を取る。
- 集計と非集計の混在: GROUP BY なしの混在はエラー（本リポジトリのビルダーで検証）。
- WHERE の集計: HAVING に移す。WHERE では集計関数を許容しない。
- DATEDIFF/FORMAT 等: ksqlDBに直接相当なし。アプリ/事前加工で補完。

## 使い方（クイック例）

文字列操作の例

```sql
-- Name を大文字化し、'foo' を含む行だけ
SELECT UPPER(Name) AS NAME_UPPER
FROM users_stream
WHERE INSTR(Name, 'foo') > 0
EMIT CHANGES;
```

時刻操作の例

```sql
-- 5分タムリング集計
SELECT WINDOWSTART AS BucketStart,
       COUNT(*) AS Cnt
FROM users_stream WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY WINDOWSTART
EMIT CHANGES;
```

条件/NULL処理の例

```sql
-- CASE / COALESCE の利用
SELECT CASE WHEN Score >= 80 THEN 'A' ELSE 'B' END AS Grade,
       COALESCE(Nickname, Name) AS DisplayName
FROM users_stream
EMIT CHANGES;
```

## 成功確認チェックリスト

- DECIMAL の精度/スケールが Avro スキーマと一致している
- 文字列比較や部分一致が `INSTR`/`LIKE` などで意図通り動いている
- 日時は `TIMESTAMP`（エポックms）で扱い、タイムゾーン差を考慮した
- 集計と非集計の混在は GROUP BY で解消、または設計を見直した
- WHERE に集計関数を入れていない（HAVING を使用）
- JSON は文字列で保持し、`JSON_EXTRACT_STRING` 等で必要箇所のみ抽出した

