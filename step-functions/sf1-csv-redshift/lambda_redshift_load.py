"""
Redshift データロードLambda
ParquetファイルをRedshiftにCOPY
"""
import json
import boto3
import os
import time
from datetime import datetime

redshift_data = boto3.client('redshift-data')

def lambda_handler(event, context):
    """
    ParquetファイルをRedshiftにロード
    """
    batch_id = event.get('batch_id')
    parquet_s3_uri = event.get('parquet_s3_uri')
    redshift_config = event.get('redshift_config', {})
    dataset = event.get('dataset', 'unknown')
    file_key = event.get('file_key', '')
    
    # Redshift設定
    workgroup = redshift_config.get('workgroup', 'default')
    database = redshift_config.get('database', 'dev')
    iam_role = redshift_config.get('iam_role', 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/redshift-copy-role')
    target_table = redshift_config.get('target_table', 'public.etl_data')
    copy_options = redshift_config.get('copy_options', 'FORMAT AS PARQUET')
    
    inserted_rows = 0
    error_message = None
    success = True
    
    try:
        # 一時的なステージングテーブル名生成
        timestamp = str(int(time.time()))
        staging_table = f"{target_table}_staging_{timestamp}"
        
        # 直接対象テーブルにCOPY
        copy_sql = f"""
        COPY {target_table}
        FROM '{parquet_s3_uri}/'
        IAM_ROLE '{iam_role}'
        {copy_options};
        """
        
        print(f"Loading Parquet data from {parquet_s3_uri} to {target_table}")
        
        # COPYコマンド実行
        copy_response = redshift_data.execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            Sql=copy_sql
        )
        
        copy_statement_id = copy_response['Id']
        wait_for_completion(copy_statement_id, workgroup, database)
        
        # 件数確認
        count_response = redshift_data.execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            Sql=f"SELECT COUNT(*) FROM {target_table};"
        )
        
        count_statement_id = count_response['Id']
        wait_for_completion(count_statement_id, workgroup, database)
        
        # 結果取得
        count_result = redshift_data.get_statement_result(Id=count_statement_id)
        if count_result['Records']:
            inserted_rows = int(count_result['Records'][0][0]['longValue'])
        
        print(f"Successfully loaded {inserted_rows} rows to {target_table}")
        
    except Exception as e:
        success = False
        error_message = str(e)
        print(f"Error loading to Redshift: {e}")
    
    # 証跡情報
    evidence = {
        "batch_id": batch_id,
        "test_id": "",
        "flow": "csv-to-parquet-pipeline",
        "step": "redshift_load",
        "input": {
            "s3": parquet_s3_uri,
            "dataset": dataset,
            "file": file_key
        },
        "output": {
            "s3": "",  # Redshift なので S3 出力なし
            "rows": inserted_rows if success else 0
        },
        "load": {
            "table": target_table,
            "inserted_rows": inserted_rows,
            "dropped_rows": 0,
            "reason": error_message if error_message else "successful load"
        },
        "ok": success,
        "ts": datetime.now().isoformat(),
        "note": error_message if error_message else f"Loaded {inserted_rows} rows into {target_table}"
    }
    
    return {
        'statusCode': 200 if success else 500,
        'batch_id': batch_id,
        'inserted_rows': inserted_rows,
        'target_table': target_table,
        'success': success,
        'evidence': evidence,
        'error': error_message
    }

def wait_for_completion(statement_id, workgroup, database, max_wait_seconds=300):
    """
    Redshift Data API ステートメント完了待機
    """
    waited = 0
    while waited < max_wait_seconds:
        response = redshift_data.describe_statement(Id=statement_id)
        status = response['Status']
        
        if status == 'FINISHED':
            return True
        elif status in ['FAILED', 'ABORTED']:
            error = response.get('Error', 'Unknown error')
            raise Exception(f"Statement failed: {error}")
        
        time.sleep(2)
        waited += 2
    
    raise Exception(f"Statement timeout after {max_wait_seconds} seconds")