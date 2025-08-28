#!/bin/bash

# ETL Evidence System デプロイスクリプト
set -e

APP_NAME="etl-observer"
STAGE="dev"
REGION="ap-northeast-1"
ACCOUNT_ID="275884879886"

# バケット名
LANDING_BUCKET="${APP_NAME}-${STAGE}-landing"
STAGING_BUCKET="${APP_NAME}-${STAGE}-staging"
EVIDENCE_BUCKET="${APP_NAME}-${STAGE}-evidence"

# リソース名
LOG_GROUP="/aws/states/${APP_NAME}-${STAGE}-central"
MONITORING_LAMBDA="lm-${APP_NAME}-${STAGE}-collector"

echo "=== Deploying ETL Evidence System ==="
echo "App: ${APP_NAME}-${STAGE}"
echo "Region: ${REGION}"

# 1. S3バケット作成
echo "Creating S3 buckets..."
aws s3 mb s3://${LANDING_BUCKET} --region ${REGION} || echo "Bucket ${LANDING_BUCKET} already exists"
aws s3 mb s3://${STAGING_BUCKET} --region ${REGION} || echo "Bucket ${STAGING_BUCKET} already exists"  
aws s3 mb s3://${EVIDENCE_BUCKET} --region ${REGION} || echo "Bucket ${EVIDENCE_BUCKET} already exists"

# 2. CloudWatch Log Group作成
echo "Creating CloudWatch Log Group..."
aws logs create-log-group --log-group-name "${LOG_GROUP}" --region ${REGION} || echo "Log group already exists"

# 3. 監視Lambda作成（最小版）
echo "Creating monitoring Lambda function..."
zip -j monitoring_lambda.zip monitoring_lambda.py

# 既存のlambda-roleを使用
LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/lambda-role"

aws lambda create-function \
    --function-name ${MONITORING_LAMBDA} \
    --runtime python3.9 \
    --role ${LAMBDA_ROLE_ARN} \
    --handler monitoring_lambda.lambda_handler \
    --zip-file fileb://monitoring_lambda.zip \
    --environment Variables="{EVIDENCE_BUCKET=${EVIDENCE_BUCKET},ENABLED=true,SAVE_RAW_LOGS=true}" \
    --timeout 300 \
    --memory-size 256 \
    --region ${REGION} || {
        echo "Function exists, updating code..."
        aws lambda update-function-code \
            --function-name ${MONITORING_LAMBDA} \
            --zip-file fileb://monitoring_lambda.zip \
            --region ${REGION}
    }

# 4. Lambda起動権限付与
echo "Setting up Lambda permissions..."
aws lambda add-permission \
    --function-name ${MONITORING_LAMBDA} \
    --statement-id logs-invoke-permission \
    --action lambda:InvokeFunction \
    --principal logs.amazonaws.com \
    --source-arn "arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP}:*" \
    --region ${REGION} || echo "Permission already exists"

# 5. ログサブスクリプション設定
echo "Setting up log subscription..."
LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${MONITORING_LAMBDA}"

aws logs put-subscription-filter \
    --log-group-name "${LOG_GROUP}" \
    --filter-name "${APP_NAME}-${STAGE}-monitoring-filter" \
    --filter-pattern "" \
    --destination-arn ${LAMBDA_ARN} \
    --region ${REGION} || echo "Subscription filter already exists"

# 6. テストCSVアップロード
echo "Uploading test CSV..."
aws s3 cp test_data.csv s3://${LANDING_BUCKET}/test/sample.csv

# 7. 手動でテスト用の証跡ログ投入
echo "Creating test evidence log..."
cat > test_evidence.json << 'EOF'
{
    "evidence": {
        "batch_id": "B20250826001",
        "test_id": "T01",
        "flow": "csv-to-parquet-pipeline",
        "step": "test_manual",
        "input": {
            "s3": "s3://etl-observer-dev-landing/test/sample.csv",
            "rows": 10
        },
        "output": {
            "s3": "s3://etl-observer-dev-staging/test/sample.parquet",
            "rows": 10
        },
        "load": {
            "table": "public.test_table",
            "inserted_rows": 10,
            "dropped_rows": 0,
            "reason": "successful test load"
        },
        "ok": true,
        "ts": "2025-08-26T22:00:00Z",
        "note": "Manual test evidence"
    }
}
EOF

# テスト用ログイベントを作成（シミュレーション）
aws logs put-log-events \
    --log-group-name "${LOG_GROUP}" \
    --log-stream-name "test-stream-$(date +%s)" \
    --log-events "timestamp=$(date +%s000),message=EVIDENCE $(cat test_evidence.json)" \
    --region ${REGION} || echo "Creating log stream first..."

# ログストリーム作成してからリトライ
LOG_STREAM="test-stream-$(date +%s)"
aws logs create-log-stream \
    --log-group-name "${LOG_GROUP}" \
    --log-stream-name "${LOG_STREAM}" \
    --region ${REGION}

aws logs put-log-events \
    --log-group-name "${LOG_GROUP}" \
    --log-stream-name "${LOG_STREAM}" \
    --log-events "timestamp=$(date +%s000),message=EVIDENCE $(cat test_evidence.json)" \
    --region ${REGION}

echo ""
echo "=== Deployment Complete ==="
echo "Landing Bucket: s3://${LANDING_BUCKET}"
echo "Evidence Bucket: s3://${EVIDENCE_BUCKET}" 
echo "Monitoring Lambda: ${MONITORING_LAMBDA}"
echo "Log Group: ${LOG_GROUP}"
echo ""
echo "=== Testing ==="
echo "1. Check evidence bucket for generated reports:"
echo "   aws s3 ls s3://${EVIDENCE_BUCKET}/evidence/ --recursive"
echo ""
echo "2. Test the monitoring system by putting more logs"
echo "3. View generated HTML report in evidence bucket"

# クリーンアップ
rm -f monitoring_lambda.zip test_evidence.json