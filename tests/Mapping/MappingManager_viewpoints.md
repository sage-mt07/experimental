# MappingManager Viewpoints

## Edge Cases & Failure Scenarios
- 未登録のエンティティを `ExtractKeyValue` に渡すと `KeyNotFoundException` を返すか
- キープロパティが `null` の場合は例外を投げて Fail-Fast となるか
- 複合キーの一部が欠損しているときの挙動
- `Register` を複数回呼び出した際のモデル上書き可否
- 不正な `EntityModel` (Key 未定義) を登録した場合のバリデーション

## Integration Patterns
- `KsqlContext` との組み合わせで key/value を生成し Kafka 送信まで到達するか
- Mapping 未登録の型を使った場合にパイプライン全体がどの段階でエラーを返すか

このファイルは `docs/structure/shion/key_value_flow_shion.md` と同期し、
MappingManager のテスト設計指針を詳細に記録します。
