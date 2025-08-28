# AWS ETL Evidence System

完全自動化されたETLパイプラインと証跡監視システム

## 🎯 システム概要

3つのStep Functionsによる自動化ETLパイプライン：
- **SF1: CSV→Redshift** ✅ 本番運用可能
- **SF2: JSON→DynamoDB** ✅ 動作確認完了  
- **SF3: ログ→Athena** ✅ 動作確認完了

全工程の**自動証跡収集**・**HTMLレポート生成**・**CloudWatch統合**

## 🏗️ アーキテクチャ

```
S3 → EventBridge → Step Functions → 各種AWSサービス
                        ↓
        監視Lambda → 証跡収集 → JSON/HTMLレポート自動生成
```

## 📁 プロジェクト構造

```
etl-evidence-system/
├── step-functions/          # Step Functions定義とLambda関数
│   ├── sf1-csv-redshift/   # CSV→Redshift パイプライン
│   ├── sf2-json-dynamodb/  # JSON→DynamoDB パイプライン
│   └── sf3-log-athena/     # ログ→Athena パイプライン
├── lambda-functions/        # Lambda関数・デプロイパッケージ
├── config/                  # 設定ファイル・IAMポリシー
├── docs/                    # ドキュメント・引き継ぎ文書
├── deployment/              # デプロイスクリプト
└── tests/                   # テストデータ
```

## 🚀 クイックスタート

### 前提条件
- AWS CLI設定済み
- Python 3.9+
- 適切なIAM権限

### 基本セットアップ
```bash
# 1. IAMロール作成
aws iam create-role --role-name lambda-role --assume-role-policy-document file://config/lambda-trust-policy.json

# 2. Step Functions実行権限設定
aws iam create-role --role-name stepfunctions-execution-role --assume-role-policy-document file://config/stepfunctions-trust-policy.json

# 3. 監視Lambda デプロイ
aws lambda create-function --function-name lm-etl-observer-dev-collector \
  --runtime python3.9 --role arn:aws:iam::YOUR_ACCOUNT:role/lambda-role \
  --handler monitoring_lambda.lambda_handler \
  --zip-file fileb://lambda-functions/monitoring_lambda.zip
```

### Step Functions 実行例
```bash
# CSV→Redshift パイプライン
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:region:account:stateMachine:sf-etl-observer-dev-ingest" \
  --input file://tests/csv_input_sample.json

# JSON→DynamoDB パイプライン  
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:region:account:stateMachine:sf-json-processor-dev-ingest" \
  --input file://tests/json_input_sample.json
```

## 📊 監視・レポート機能

### 自動証跡収集
- 全Step Functions実行ログの自動収集
- エラー発生時の詳細情報記録
- S3への証跡ファイル自動保存

### HTMLレポート自動生成
- 実行統計・成功/失敗率
- エラー詳細・トラブルシューティング情報
- JSON形式の生データアクセス

### 最適化機能
- **ページング対応**: 大量ファイル処理時の安定性向上
- **終端確定化**: コスト効率改善（途中経過での無駄な集計を削減）

## 💡 主要機能

### 1. CSV→Redshift パイプライン (SF1)
- S3 CSV自動検知 → Glue変換 → Redshift書き込み
- データ品質チェック・バリデーション
- 完全自動化・本番運用レベル

### 2. JSON→DynamoDB パイプライン (SF2)  
- JSON構造検証・前処理
- DynamoDB書き込み・重複チェック
- エラーハンドリング・リトライ機能

### 3. ログ→Athena パイプライン (SF3)
- CloudWatch Logs収集
- Glue Crawler自動実行
- Athena クエリ実行・結果保存

## 📋 設定・カスタマイズ

### フロー設定
`config/flow_mapping.json` でパイプライン識別ルール設定

### 環境変数
- `EVIDENCE_BUCKET`: 証跡保存S3バケット
- `ENABLED`: 監視機能ON/OFF
- `SAVE_RAW_LOGS`: 生ログ保存設定

## 🔧 運用・メンテナンス

### ログ確認
```bash
# Step Functions実行状況
aws stepfunctions list-executions --state-machine-arn [ARN]

# Lambda実行ログ  
aws logs describe-log-streams --log-group-name "/aws/lambda/lm-etl-observer-dev-collector"
```

### トラブルシューティング
1. Step Functions実行履歴確認
2. Lambda CloudWatch Logs確認
3. S3証跡ファイル確認
4. IAM権限確認

## 💰 コスト最適化

### 推定月額コスト
- **$5-15/月** (軽度使用時)
- 主なコスト要因: S3ストレージ（証跡ファイル蓄積）

### コスト削減方法
- S3ライフサイクルポリシー設定
- 不要なLambda関数削除
- Step Functions実行頻度調整

## 📚 ドキュメント

詳細情報は `docs/` フォルダ参照：
- `01_引継ぎ文書_ETLエビデンスシステム_基盤完成.md`
- `02_引継ぎ文書_Step_Functions拡張作業_20250827.md`

## 🤝 貢献・開発

### 開発環境セットアップ
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install boto3 pandas
```

### コード規約
- PEP 8準拠
- 型アノテーション推奨
- 日本語コメント・ドキュメント

## ⚠️ セキュリティ

- AWS認証情報をコードに含めない
- IAM最小権限の原則
- S3バケット暗号化設定
- VPC内通信推奨

## 📄 ライセンス

MIT License

---

## ⚙️ 設定方法

**重要**: 全ての設定は`config/system_config.json`の**1箇所のみ**で管理されます。

### 初期設定
```bash
# 1. 設定ファイル編集
vim config/system_config.json

# 2. AWS情報を更新
{
  "aws": {
    "account_id": "123456789012",  # ← あなたのAWSアカウントID
    "region": "ap-northeast-1"     # ← あなたのAWSリージョン
  }
}
```

これだけで全てのARN・リソース名が自動生成されます。

---

**作成者**: Claude AI Assistant  
**最終更新**: 2025-08-28  
**バージョン**: v0.1 - 統一設定対応版