"""
ETL Evidence System デプロイスクリプト
AWS CLI を使用してインフラとLambda関数をデプロイ
"""
import json
import boto3
import subprocess
import os
import zipfile
import time
from datetime import datetime

# 設定
APP_NAME = "etl-observer"
STAGE = "dev"
REGION = "YOUR_AWS_REGION"
ACCOUNT_ID = "YOUR_AWS_ACCOUNT_ID"

# バケット名
LANDING_BUCKET = f"{APP_NAME}-{STAGE}-landing"
STAGING_BUCKET = f"{APP_NAME}-{STAGE}-staging"
EVIDENCE_BUCKET = f"{APP_NAME}-{STAGE}-evidence"

# リソース名
LOG_GROUP = f"/aws/states/{APP_NAME}-{STAGE}-central"
STEP_FUNCTION_NAME = f"sf-{APP_NAME}-{STAGE}-ingest"
GLUE_JOB_NAME = f"glue-{APP_NAME}-{STAGE}-csv2parquet"
MONITORING_LAMBDA = f"lm-{APP_NAME}-{STAGE}-collector"

# Lambda関数名
PREVALIDATE_LAMBDA = f"{APP_NAME}-{STAGE}-prevalidate"
REDSHIFT_LAMBDA = f"{APP_NAME}-{STAGE}-redshift-load"
FINALIZE_LAMBDA = f"{APP_NAME}-{STAGE}-finalize"

# AWS クライアント
s3 = boto3.client('s3', region_name=REGION)
iam = boto3.client('iam', region_name=REGION)
lambda_client = boto3.client('lambda', region_name=REGION)
logs_client = boto3.client('logs', region_name=REGION)
states_client = boto3.client('stepfunctions', region_name=REGION)
glue_client = boto3.client('glue', region_name=REGION)

def run_command(command, description):
    """コマンド実行"""
    print(f"\n=== {description} ===")
    print(f"Command: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

def create_s3_buckets():
    """S3バケット作成"""
    buckets = [LANDING_BUCKET, STAGING_BUCKET, EVIDENCE_BUCKET]
    
    for bucket_name in buckets:
        try:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': REGION}
            )
            print(f"Created S3 bucket: {bucket_name}")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e):
                print(f"S3 bucket already exists: {bucket_name}")
            else:
                print(f"Error creating bucket {bucket_name}: {e}")

def create_iam_roles():
    """IAMロール作成"""
    
    # Lambda実行ロール
    lambda_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    lambda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream", 
                    "logs:PutLogEvents",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement", 
                    "redshift-data:GetStatementResult"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName=f"{APP_NAME}-{STAGE}-lambda-role",
            AssumeRolePolicyDocument=json.dumps(lambda_trust_policy),
            Description="Lambda execution role for ETL evidence system"
        )
        
        iam.put_role_policy(
            RoleName=f"{APP_NAME}-{STAGE}-lambda-role",
            PolicyName="LambdaExecutionPolicy",
            PolicyDocument=json.dumps(lambda_policy)
        )
        
        print(f"Created IAM role: {APP_NAME}-{STAGE}-lambda-role")
    except Exception as e:
        if "already exists" in str(e):
            print(f"IAM role already exists: {APP_NAME}-{STAGE}-lambda-role")
        else:
            print(f"Error creating IAM role: {e}")
    
    # Step Functions実行ロール
    sf_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    sf_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "lambda:InvokeFunction",
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:BatchStopJobRun",
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName=f"{APP_NAME}-{STAGE}-stepfunctions-role",
            AssumeRolePolicyDocument=json.dumps(sf_trust_policy),
            Description="Step Functions execution role for ETL evidence system"
        )
        
        iam.put_role_policy(
            RoleName=f"{APP_NAME}-{STAGE}-stepfunctions-role",
            PolicyName="StepFunctionsExecutionPolicy",
            PolicyDocument=json.dumps(sf_policy)
        )
        
        print(f"Created IAM role: {APP_NAME}-{STAGE}-stepfunctions-role")
    except Exception as e:
        if "already exists" in str(e):
            print(f"IAM role already exists: {APP_NAME}-{STAGE}-stepfunctions-role")
        else:
            print(f"Error creating Step Functions role: {e}")

def create_zip_file(file_path, zip_name):
    """Lambda用ZIPファイル作成"""
    with zipfile.ZipFile(zip_name, 'w') as zip_file:
        zip_file.write(file_path, os.path.basename(file_path))
    return zip_name

def deploy_lambda_functions():
    """Lambda関数デプロイ"""
    lambda_configs = [
        {
            'name': MONITORING_LAMBDA,
            'file': 'monitoring_lambda.py',
            'handler': 'monitoring_lambda.lambda_handler',
            'env_vars': {
                'EVIDENCE_BUCKET': EVIDENCE_BUCKET,
                'ENABLED': 'true',
                'SAVE_RAW_LOGS': 'true'
            }
        },
        {
            'name': PREVALIDATE_LAMBDA,
            'file': 'lambda_prevalidate.py',
            'handler': 'lambda_prevalidate.lambda_handler',
            'env_vars': {}
        },
        {
            'name': REDSHIFT_LAMBDA,
            'file': 'lambda_redshift_load.py', 
            'handler': 'lambda_redshift_load.lambda_handler',
            'env_vars': {}
        },
        {
            'name': FINALIZE_LAMBDA,
            'file': 'lambda_finalize.py',
            'handler': 'lambda_finalize.lambda_handler',
            'env_vars': {}
        }
    ]
    
    lambda_role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{APP_NAME}-{STAGE}-lambda-role"
    
    for config in lambda_configs:
        zip_file = create_zip_file(config['file'], f"{config['name']}.zip")
        
        try:
            # Lambda関数作成
            with open(zip_file, 'rb') as zip_content:
                lambda_client.create_function(
                    FunctionName=config['name'],
                    Runtime='python3.9',
                    Role=lambda_role_arn,
                    Handler=config['handler'],
                    Code={'ZipFile': zip_content.read()},
                    Environment={'Variables': config['env_vars']},
                    Timeout=300,
                    MemorySize=256
                )
            print(f"Created Lambda function: {config['name']}")
            
        except Exception as e:
            if "Function already exist" in str(e):
                # 既存の場合は更新
                with open(zip_file, 'rb') as zip_content:
                    lambda_client.update_function_code(
                        FunctionName=config['name'],
                        ZipFile=zip_content.read()
                    )
                print(f"Updated Lambda function: {config['name']}")
            else:
                print(f"Error deploying Lambda {config['name']}: {e}")
        
        # ZIPファイル削除
        os.remove(zip_file)

def create_log_group():
    """CloudWatch Log Group作成"""
    try:
        logs_client.create_log_group(logGroupName=LOG_GROUP)
        print(f"Created log group: {LOG_GROUP}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Log group already exists: {LOG_GROUP}")
        else:
            print(f"Error creating log group: {e}")

def setup_log_subscription():
    """ログサブスクリプション設定"""
    monitoring_lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{MONITORING_LAMBDA}"
    
    try:
        # Lambda起動権限付与
        lambda_client.add_permission(
            FunctionName=MONITORING_LAMBDA,
            StatementId='logs-invoke-permission',
            Action='lambda:InvokeFunction',
            Principal='logs.amazonaws.com',
            SourceArn=f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{LOG_GROUP}:*"
        )
        
        # サブスクリプションフィルター作成
        logs_client.put_subscription_filter(
            logGroupName=LOG_GROUP,
            filterName=f"{APP_NAME}-{STAGE}-monitoring-filter",
            filterPattern='',  # 全ログを対象
            destinationArn=monitoring_lambda_arn
        )
        
        print(f"Created log subscription from {LOG_GROUP} to {MONITORING_LAMBDA}")
        
    except Exception as e:
        print(f"Error setting up log subscription: {e}")

def deploy_step_functions():
    """Step Functions作成"""
    sf_role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{APP_NAME}-{STAGE}-stepfunctions-role"
    
    # Step Functions定義を読み込み
    with open('step_functions_definition.json', 'r') as f:
        definition = f.read()
    
    # ARNを実際の値に置換
    definition = definition.replace('etl-observer-dev-prevalidate', PREVALIDATE_LAMBDA)
    definition = definition.replace('etl-observer-dev-redshift-load', REDSHIFT_LAMBDA)
    definition = definition.replace('etl-observer-dev-finalize', FINALIZE_LAMBDA)
    definition = definition.replace('glue-etl-observer-dev-csv2parquet', GLUE_JOB_NAME)
    
    try:
        states_client.create_state_machine(
            name=STEP_FUNCTION_NAME,
            definition=definition,
            roleArn=sf_role_arn,
            type='STANDARD',
            loggingConfiguration={
                'level': 'ALL',
                'includeExecutionData': True,
                'destinations': [
                    {
                        'cloudWatchLogsLogGroup': {
                            'logGroupArn': f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{LOG_GROUP}:*"
                        }
                    }
                ]
            }
        )
        print(f"Created Step Functions: {STEP_FUNCTION_NAME}")
        
    except Exception as e:
        if "already exists" in str(e):
            print(f"Step Functions already exists: {STEP_FUNCTION_NAME}")
        else:
            print(f"Error creating Step Functions: {e}")

def main():
    """メインデプロイ処理"""
    print(f"=== Deploying ETL Evidence System ===")
    print(f"App: {APP_NAME}-{STAGE}")
    print(f"Region: {REGION}")
    print(f"Account: {ACCOUNT_ID}")
    
    # デプロイ手順
    create_s3_buckets()
    create_iam_roles()
    
    # IAMロール反映待機
    print("Waiting for IAM roles to propagate...")
    time.sleep(10)
    
    deploy_lambda_functions()
    create_log_group()
    setup_log_subscription()
    deploy_step_functions()
    
    print("\n=== Deployment Summary ===")
    print(f"Landing Bucket: s3://{LANDING_BUCKET}")
    print(f"Staging Bucket: s3://{STAGING_BUCKET}")
    print(f"Evidence Bucket: s3://{EVIDENCE_BUCKET}")
    print(f"Step Functions: {STEP_FUNCTION_NAME}")
    print(f"Monitoring Lambda: {MONITORING_LAMBDA}")
    print(f"Log Group: {LOG_GROUP}")
    
    print("\n=== Next Steps ===")
    print("1. Upload test CSV to landing bucket")
    print("2. Execute Step Functions manually or via S3 event")
    print("3. Check evidence bucket for results")

if __name__ == "__main__":
    main()