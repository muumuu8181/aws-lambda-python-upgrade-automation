"""
ETL前処理Lambda - バリデーションとルーティング
"""
import json
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    CSVファイルの存在確認とバリデーション
    """
    batch_id = event.get('batch_id')
    files = event.get('files', [])
    
    validated_files = []
    errors = []
    total_size = 0
    
    try:
        for file_input in files:
            bucket = file_input['bucket']
            key = file_input['key']
            
            try:
                # S3オブジェクトの存在確認
                response = s3.head_object(Bucket=bucket, Key=key)
                file_size = response['ContentLength']
                total_size += file_size
                
                # ファイルサイズチェック（例: 1GB制限）
                if file_size > 1024 * 1024 * 1024:
                    errors.append(f"File too large: {key} ({file_size} bytes)")
                    continue
                
                # CSVファイル拡張子チェック
                if not key.lower().endswith('.csv'):
                    errors.append(f"Not a CSV file: {key}")
                    continue
                
                validated_files.append({
                    **file_input,
                    'file_size': file_size,
                    'last_modified': response['LastModified'].isoformat()
                })
                
            except Exception as e:
                errors.append(f"Error accessing {key}: {str(e)}")
        
        success = len(errors) == 0 and len(validated_files) > 0
        
        # 証跡情報
        evidence = {
            "batch_id": batch_id,
            "test_id": "",
            "flow": "csv-to-parquet-pipeline",
            "step": "prevalidate",
            "input": {
                "files_count": len(files),
                "total_size_bytes": total_size
            },
            "output": {
                "validated_files": len(validated_files),
                "errors": errors
            },
            "load": {},
            "ok": success,
            "ts": datetime.now().isoformat(),
            "note": f"Validated {len(validated_files)} files, {len(errors)} errors"
        }
        
        return {
            'statusCode': 200,
            'batch_id': batch_id,
            'validated_files': validated_files,
            'validation_errors': errors,
            'success': success,
            'evidence': evidence
        }
        
    except Exception as e:
        error_evidence = {
            "batch_id": batch_id,
            "step": "prevalidate",
            "ok": False,
            "error": str(e),
            "ts": datetime.now().isoformat()
        }
        
        return {
            'statusCode': 500,
            'error': str(e),
            'evidence': error_evidence
        }