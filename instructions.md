# Codex作業補助：設計原則と命名ルール

## 命名規則
- エンティティ名：PascalCase
- Kafkaトピック名：kebab-case（自動変換規則あり）

## 設計原則
- LINQからKSQL変換：式ツリーベース解析を用いる
- DLQ送信：OnError(ErrorAction.DLQ) にて宣言的に記述
- モジュール設計：FailFast優先／暗黙依存を排除

## 出力指針
- 出力コードはsrc/下の既存構成に従うこと
- テストコードはtests/下にmirror構成で配置する
