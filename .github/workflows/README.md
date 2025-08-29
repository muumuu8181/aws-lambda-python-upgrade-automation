# GitHub Actions ワークフロー

## Lambda Python Upgrade ワークフロー

このワークフローは、AWS Lambda関数のPythonランタイムバージョンを自動的にアップグレードし、LocalStackでテストを行います。

### 使用方法

1. **GitHub Actions タブ**から「Lambda Python Version Upgrade & Test」を選択
2. **Run workflow**をクリック
3. パラメータを設定:
   - **Target Python version**: アップグレード先のPythonバージョン (例: 3.13, 3.12)
   - **Lambda functions**: アップグレード対象の関数名 (カンマ区切り、または"all")
   - **Test mode**: LocalStackでのテスト実行するかどうか

### ワークフロー構成

1. **validate-and-prepare**: 入力パラメータの検証とLambda関数リストの準備
2. **localstack-test**: LocalStack環境での新Pythonバージョンテスト
3. **upgrade-lambda-functions**: 実際のAWS Lambda関数のランタイム更新
4. **update-step-functions**: 関連するGlue JobやStep Functionsの更新
5. **generate-report**: アップグレード結果のレポート生成

### 必要なGitHubシークレット

以下のシークレットをGitHubリポジトリに設定してください:

- `AWS_ACCESS_KEY_ID`: AWSアクセスキーID
- `AWS_SECRET_ACCESS_KEY`: AWSシークレットアクセスキー  
- `AWS_ACCOUNT_ID`: AWSアカウントID (12桁)

### LocalStack機能

- Docker servicesを使用してLocalStack環境を自動構築
- Lambda, S3, Step Functions, Glue, Redshiftサービスをエミュレート
- 本番デプロイ前の安全なテスト環境を提供

### 対応するAWSサービス

- **AWS Lambda**: Pythonランタイムバージョン更新
- **AWS Glue**: Python実行環境の更新
- **AWS Step Functions**: パイプライン実行テスト
- **S3/Redshift**: データフロー確認

### トラブルシューティング

#### よくある問題

1. **権限エラー**: AWS IAMロールの権限を確認
2. **LocalStack接続エラー**: Dockerサービスの起動を待機
3. **関数が見つからない**: Lambda関数名を確認

#### ログ確認

各ジョブのログでエラー詳細を確認できます:
- LocalStackテストログ
- Lambda更新ログ  
- Step Functions実行ログ