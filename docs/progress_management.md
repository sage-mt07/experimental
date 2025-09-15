# Progress Management Guide

## Recording Rules
Record progress, design decisions, and issues in chronological order. Obtain the current date and time from the OS in JST and include the time zone explicitly.

### Storage Location
Store daily reports under `docs/changes/` as `YYYYMMDD_progress.md`, appending entries in chronological order.

### Examples for retrieving the current time
- Windows: `echo %date% %time%`
- Linux/Mac: `date '+%Y-%m-%d %H:%M:%S %Z'`
- C#: `DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " JST"`
- Python: `datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S JST')`

### Format
```
## YYYY-MM-DD HH:mm JST [owner]
Summary of progress or meeting notes
- Bullet list of specific tasks, decisions, questions, next actions
- Include related files or references when available
- Add special notes or background as needed
```

### Sample Entry
```
## 2025-07-11 21:20 JST [naruse]
Started PR for EntityBuilder implementation. Currently auditing attribute classes slated for removal.
- Enumerating KsqlTableAttribute, TopicAttribute dependencies
- Mapping list of types to remove and their usage
```

---

## Operational Notes and Revision History
Adopted PM directive and codex proposal on 2025-07-12
- Clarified operation of progress logs (`docs/changes/`)
- Unified recording rules for diff_log (`docs/diff_log/`)
- Established rules for working under `features/{feature}` directories
- Synchronized documents and tests
- Emphasized culture of promptly sharing uncertainties with evidence
