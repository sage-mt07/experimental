# KSQL Function and Data Type Mapping (Core Functions)

This cheat sheet shows quick correspondences between SQL Server and ksqlDB types/functions.

## Data type mapping

| SQL Server | ksqlDB/Avro | Notes |
|------------|-------------|-------|
| TINYINT/SMALLINT/INT | INTEGER | Watch for range differences; adjust if overflow occurs. |
| BIGINT | BIGINT | Direct equivalent. |
| BIT | BOOLEAN | 0/1 maps to false/true. |
| DECIMAL(p,s), NUMERIC(p,s) | DECIMAL(p,s) | Precision/scale must be defined in the schema. |
| REAL/FLOAT | DOUBLE | Expect rounding differences. |
| MONEY/SMALLMONEY | DECIMAL(19,4) equivalent | Normalize to DECIMAL by policy. |
| CHAR/NCHAR/VARCHAR/NVARCHAR | VARCHAR | UTF-8 assumed; normalize as needed. |
| BINARY/VARBINARY | BYTES | Binary may appear in Base64 form. |
| UNIQUEIDENTIFIER | VARCHAR or BYTES | Handling as string GUID is simpler. |
| DATE | DATE | Direct equivalent. |
| TIME | TIME | Direct equivalent. |
| DATETIME/SMALLDATETIME/DATETIME2 | TIMESTAMP | Epoch ms basis; mind time zones. |
| XML/JSON | VARCHAR | Treat JSON as string; use JSON_* functions to extract. |
| GEOGRAPHY/GEOMETRY | VARCHAR/STRUCT | Convert to GeoJSON or similar before storage. |

## String functions

| SQL Server | ksqlDB | Example |
|------------|--------|---------|
| UPPER | UPPER | `UPPER(Name)` |
| LOWER | LOWER | `LOWER(Name)` |
| SUBSTRING | SUBSTRING | `SUBSTRING(Name, 1, 3)` |
| LEN | LEN | `LEN(Name)` (note trailing-space handling differences) |
| TRIM | TRIM | `TRIM(Name)` |
| REPLACE | REPLACE | `REPLACE(Name,'a','b')` |
| CHARINDEX | INSTR | `INSTR(Name,'foo')` |
| CONCAT | CONCAT | `CONCAT(A,B,...)` |

## Numeric and math functions

| SQL Server | ksqlDB | Example |
|------------|--------|---------|
| ABS | ABS | `ABS(x)` |
| ROUND | ROUND | `ROUND(x[,d])` |
| FLOOR | FLOOR | `FLOOR(x)` |
| CEILING | CEIL | `CEIL(x)` |
| SQRT | SQRT | `SQRT(x)` |
| POWER | POWER | `POWER(x,y)` |
| SIGN | SIGN | `SIGN(x)` |
| LOG/LOG10 | LOG/LOG10 | `LOG(x)`, `LOG10(x)` |
| EXP | EXP | `EXP(x)` |

## Date and time

| SQL Server | ksqlDB | Example / Notes |
|------------|--------|-----------------|
| DATEADD | DATEADD | `DATEADD('minute', 5, ts)` (equivalent to AddMinutes) |
| DATEDIFF | — (use arithmetic) | Compute difference in ms manually (`ts2 - ts1`). |
| YEAR/MONTH/DAY | YEAR/MONTH/DAY | `YEAR(ts)` etc. |
| HOUR/MINUTE/SECOND | HOUR/MINUTE/SECOND | Direct equivalent. |
| GETDATE()/SYSDATETIME() | Not recommended / unsupported | Use ROWTIME; preprocess if needed. |
| FORMAT | — | Format on the application side. |

## Aggregation and sets

| SQL Server | ksqlDB | Notes |
|------------|--------|-------|
| COUNT/SUM/AVG/MIN/MAX | COUNT/SUM/AVG/MIN/MAX | Direct equivalents. |
| COUNT(DISTINCT) | COUNT_DISTINCT | Watch performance. |
| TOP/ROW_NUMBER | TOPK/TOPKDISTINCT | Precise ranking requires additional design. |

## JSON

| SQL Server | ksqlDB | Example |
|------------|--------|---------|
| JSON_VALUE | JSON_EXTRACT_STRING | `JSON_EXTRACT_STRING(col,'$.path')` |
| JSON_QUERY | JSON_RECORDS | `JSON_RECORDS(col)` |
| JSON_ARRAY_LENGTH | JSON_ARRAY_LENGTH | Direct equivalent. |
| JSON_KEYS | JSON_KEYS | Direct equivalent. |

## Conditions and conversion

| SQL Server | ksqlDB | Example |
|------------|--------|---------|
| CASE WHEN | CASE | `CASE WHEN cond THEN a ELSE b END` |
| COALESCE | COALESCE | `COALESCE(a,b,...)` |
| ISNULL | IFNULL | `IFNULL(a,b)` |
| CAST/CONVERT | CAST | `CAST(x AS VARCHAR/INTEGER/DOUBLE/DECIMAL(...))` |

## Common pitfalls
- Line breaks and whitespace: normalize newline differences (CRLF vs. LF) when comparing generated SQL.
- Decimal precision: keep DECIMAL(p,s) identical between schema and application.
- Mixed aggregates and non-aggregates: use `GROUP BY` (the builder validates this).
- Aggregates in WHERE: move them to `HAVING`; `WHERE` does not accept aggregates.
- DATEDIFF/FORMAT: no direct equivalent; handle in the application or upstream processing.

## Quick examples

String manipulation:

```sql
-- Uppercase Name and filter rows containing 'foo'
SELECT UPPER(Name) AS NAME_UPPER
FROM users_stream
WHERE INSTR(Name, 'foo') > 0
EMIT CHANGES;
```

Time operations:

```sql
-- 5-minute tumbling aggregation
SELECT WINDOWSTART AS BucketStart,
       COUNT(*) AS Cnt
FROM users_stream WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY WINDOWSTART
EMIT CHANGES;
```

Conditions / NULL handling:

```sql
-- CASE / COALESCE usage
SELECT CASE WHEN Score >= 80 THEN 'A' ELSE 'B' END AS Grade,
       COALESCE(Nickname, Name) AS DisplayName
FROM users_stream
EMIT CHANGES;
```

## Success checklist
- DECIMAL precision/scale matches the Avro schema.
- String comparisons and partial matches (INSTR/LIKE) behave as intended.
- Datetime handling uses `TIMESTAMP` (epoch ms) with the correct time zone.
- Mixed aggregates vs. non-aggregates are resolved with `GROUP BY` or redesign.
- Aggregates are not placed in `WHERE` (use `HAVING`).
- JSON is stored as string and extracted with `JSON_EXTRACT_STRING` or related functions.
