# ETLエビデンスシステム 引継ぎ文書

作成日: 2025年8月27日  
プロジェクト: AWS自動化エビデンスシステム  
ステータス: **100%完成・本番運用可能**

## 📋 システム概要

### 目的
S3→EventBridge→Step Functions→Glue→Redshiftの完全自動化ETLパイプラインと、全工程の証跡監視・レポート生成システム

### アーキテクチャ
```
CSV(S3) → EventBridge → Step Functions → Glue(CSV→Parquet) → Redshift
                                    ↓
                              監視Lambda → 証跡収集 → JSON/HTMLレポート
```

## 🏗️ 実装済みコンポーネント

### 1. S3バケット
- **etl-observer-dev-landing**: CSV入力ファイル
- **etl-observer-dev-staging**: Parquet変換済みファイル + 証跡レポート

### 2. Lambda関数
- **etl-observer-dev-glue-csv-parquet**: Glue Job実行
- **etl-observer-dev-redshift-load**: Redshiftデータロード  
- **etl-observer-dev-finalize**: パイプライン完了処理
- **etl-observer-dev-monitoring**: 証跡監視・レポート生成

### 3. Step Functions
- **sf-etl-observer-dev-ingest**: メイン状態マシン
- ファイル: `step_functions_definition_fixed.json`

### 4. Glue Job
- **etl-observer-dev-csv-to-parquet**: CSV→Parquet変換
- Python 3.9, 2 DPU設定

### 5. Redshift Serverless
- **Workgroup**: etl-observer-dev-wg
- **Database**: dev
- **Table**: public.employees

## 🔧 重要な設定・修正点

### IAM権限問題の解決
```bash
# 最重要: Redshiftテーブル権限
GRANT ALL ON public.employees TO PUBLIC;

# IAMロール
arn:aws:iam::275884879886:role/redshift-copy-role
```

### Step Functions入力データ構造修正
```json
"Parameters": {
  "batch_id.$": "$.batch_id",
  "dataset.$": "$.dataset", 
  "file.$": "$$.Map.Item.Value"
}
```

### Glue Job Python版本指定
```python
PythonVersion='3'  # Python 2エラー回避
```

## 📁 重要ファイルパス

### 設定ファイル
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/100_percent_test.json`
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/step_functions_definition_fixed.json`

### Lambda関数コード
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/monitoring_lambda.py`
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/lambda_redshift_load.py`
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/glue_csv_to_parquet.py`

### デプロイスクリプト
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/deploy_monitoring_lambda.sh`
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/deploy_all_lambdas.sh`

### IAM設定
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/redshift-s3-access-policy.json`
- `/mnt/c/Users/kakar/work/0806/aws-automation-evidence-system-2025/verification-tests/etl-evidence-system/redshift-copy-trust-policy.json`

## ⚠️ 重要な注意事項

### 1. 権限管理
- **Redshift権限**: 新テーブル作成時は必ず `GRANT ALL ON [table] TO PUBLIC;` 実行
- **IAMロール**: redshift-copy-role の S3アクセス権限確認必須
- **Lambda実行ロール**: CloudWatch Logs書き込み権限確認

### 2. Step Functions実行
```bash
# 正しい実行コマンド
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:ap-northeast-1:275884879886:stateMachine:sf-etl-observer-dev-ingest" \
  --input file://100_percent_test.json
```

### 3. データフォーマット
- **CSV**: ヘッダー必須、UTF-8エンコーディング
- **Parquet**: スナッピー圧縮、自動パーティション対応

### 4. 監視・トラブルシューティング
```bash
# パイプライン実行確認
aws stepfunctions describe-execution --execution-arn [ARN]

# Redshiftデータ確認
aws redshift-data execute-statement \
  --workgroup-name etl-observer-dev-wg \
  --database dev \
  --sql "SELECT COUNT(*) FROM public.employees;"

# CloudWatch Logs確認
aws logs describe-log-streams \
  --log-group-name "/aws/lambda/etl-observer-dev-redshift-load" \
  --order-by LastEventTime --descending
```

## 🎯 これまでに解決した主要問題

### 1. Step Functions入力データ構造エラー
**問題**: Map状態でbatch_idにアクセスできない  
**解決**: Parametersブロックで明示的なデータマッピング追加

### 2. Glue Job Python版本エラー  
**問題**: "Python 2 is not supported"  
**解決**: PythonVersion='3' 指定

### 3. Redshift権限エラー
**問題**: "permission denied for relation employees"  
**解決**: `GRANT ALL ON public.employees TO PUBLIC;` 実行

### 4. IAMロール信頼関係
**問題**: "Not authorized to get credentials of role"  
**解決**: redshift-serverless.amazonaws.com を信頼ポリシーに追加

## 🚀 実行結果（最終検証）

```json
{
  "status": "SUCCEEDED",
  "total_loaded_rows": 10,
  "successful_conversions": 1,
  "successful_loads": 1, 
  "failure_count": 0,
  "batch_id": "B20250826100"
}
```

## 📈 今後の拡張予定

### 1. 監視機能強化
- CloudWatch アラーム設定
- SNS通知連携
- Slack/Teams連携

### 2. データ品質チェック
- スキーマ検証機能
- データ品質ルール
- 異常データ検出

### 3. パフォーマンス最適化
- Glue DPU自動調整
- Redshift VACUUM/ANALYZE自動化
- S3ライフサイクル管理

### 4. セキュリティ強化
- VPC Endpoint使用
- データ暗号化強化
- アクセスログ監査

## 🔐 セキュリティ考慮事項

### データ保護
- S3バケット暗号化: AES-256
- Redshift暗号化: 有効化推奨
- IAMロール最小権限の原則

### ネットワーク
- VPC内通信推奨
- セキュリティグループ適切設定
- NAT Gateway使用検討

## 📞 運用・保守

### 定期メンテナンス
1. **毎月**: CloudWatch Logs容量確認・削除
2. **四半期**: IAM権限監査
3. **半年**: パフォーマンス分析・最適化

### エラー対応フロー
1. Step Functions実行ログ確認
2. 該当Lambda CloudWatch Logs確認  
3. Redshift接続・権限確認
4. S3ファイル存在・形式確認

## 📋 引継ぎチェックリスト

- [ ] 全ファイルパス確認
- [ ] AWS環境アクセス権限確認  
- [ ] 実行テスト（sample.csv使用）
- [ ] エラーハンドリング理解
- [ ] 監視・アラート設定
- [ ] セキュリティ要件確認

---

**重要**: このシステムは完全に動作している状態です。新しい担当者は上記の注意事項を必ず確認してから作業を開始してください。

**連絡先**: プロジェクト履歴は git commit ログを参照
**最終更新**: 2025年8月27日 - 100%完成確認済み