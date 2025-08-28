"""
ログ収集Lambda - Step Functions 3用
S3からログファイルを収集・検証・集約処理
"""
import json
import boto3
import re
from datetime import datetime
from typing import Dict, Any, List
from urllib.parse import unquote

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """ログ収集メイン関数"""
    print(f"ログ収集開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id', f'LOG_{datetime.now().strftime("%Y%m%d%H%M")}')
        source_bucket = event.get('source_bucket', 'log-processing-dev-source')
        log_prefix = event.get('log_prefix', 'application-logs/')
        
        # ログファイル一覧取得
        log_files = list_log_files(source_bucket, log_prefix)
        
        if not log_files:
            return {
                'statusCode': 404,
                'batch_id': batch_id,
                'success': False,
                'error': f'ログファイルが見つかりません: s3://{source_bucket}/{log_prefix}',
                'evidence': create_evidence(batch_id, 'log_collect', False, {
                    'error': 'ログファイルなし',
                    'source': f's3://{source_bucket}/{log_prefix}'
                })
            }
        
        # ログファイル処理
        processed_logs = []
        total_lines = 0
        error_lines = 0
        
        for log_file in log_files[:10]:  # 最大10ファイル処理
            try:
                log_content = process_log_file(source_bucket, log_file['key'])
                processed_logs.append({
                    'file': log_file['key'],
                    'size': log_file['size'],
                    'lines': log_content['lines'],
                    'errors': log_content['errors'],
                    'summary': log_content['summary']
                })
                total_lines += log_content['lines']
                error_lines += log_content['errors']
                
            except Exception as e:
                print(f"ログファイル処理エラー {log_file['key']}: {e}")
                processed_logs.append({
                    'file': log_file['key'],
                    'error': str(e)
                })
        
        # 集約統計
        aggregated_stats = {
            'total_files': len(log_files),
            'processed_files': len([log for log in processed_logs if 'error' not in log]),
            'total_log_lines': total_lines,
            'error_log_lines': error_lines,
            'error_rate': error_lines / total_lines if total_lines > 0 else 0
        }
        
        result = {
            'statusCode': 200,
            'batch_id': batch_id,
            'success': True,
            'source_bucket': source_bucket,
            'log_prefix': log_prefix,
            'processed_logs': processed_logs,
            'aggregated_stats': aggregated_stats,
            'evidence': create_evidence(batch_id, 'log_collect', True, {
                'input': f's3://{source_bucket}/{log_prefix}',
                'files_processed': len(processed_logs),
                'total_lines': total_lines,
                'error_lines': error_lines
            })
        }
        
        print(f"ログ収集完了: {len(processed_logs)}ファイル, {total_lines}行処理")
        return result
        
    except Exception as e:
        error_msg = f"ログ収集エラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, 'log_collect', False, {'error': error_msg})
        }

def list_log_files(bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """S3からログファイル一覧取得"""
    log_files = []
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        for obj in response.get('Contents', []):
            if obj['Key'].endswith(('.log', '.txt')) and obj['Size'] > 0:
                log_files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
                
    except Exception as e:
        print(f"ログファイル一覧取得エラー: {e}")
    
    return sorted(log_files, key=lambda x: x['last_modified'], reverse=True)

def process_log_file(bucket: str, key: str) -> Dict[str, Any]:
    """個別ログファイル処理"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8', errors='ignore')
        
        lines = content.split('\n')
        total_lines = len([line for line in lines if line.strip()])
        
        # ログレベル分析
        log_levels = {'ERROR': 0, 'WARN': 0, 'INFO': 0, 'DEBUG': 0, 'OTHER': 0}
        
        for line in lines:
            if 'ERROR' in line.upper():
                log_levels['ERROR'] += 1
            elif 'WARN' in line.upper():
                log_levels['WARN'] += 1
            elif 'INFO' in line.upper():
                log_levels['INFO'] += 1
            elif 'DEBUG' in line.upper():
                log_levels['DEBUG'] += 1
            elif line.strip():
                log_levels['OTHER'] += 1
        
        return {
            'lines': total_lines,
            'errors': log_levels['ERROR'] + log_levels['WARN'],
            'summary': log_levels
        }
        
    except Exception as e:
        print(f"ログファイル処理エラー {key}: {e}")
        return {
            'lines': 0,
            'errors': 0,
            'summary': {},
            'processing_error': str(e)
        }

def create_evidence(batch_id: str, step: str, success: bool, details: Dict[str, Any]) -> Dict[str, Any]:
    """エビデンス作成"""
    note = f"ログ収集{'成功' if success else '失敗'}"
    if details.get('files_processed'):
        note += f": {details['files_processed']}ファイル"
    if details.get('total_lines'):
        note += f", {details['total_lines']}行処理"
    
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'log-aggregation-athena-pipeline',
        'step': step,
        'input': {'s3': details.get('input', ''), 'files': details.get('files_processed', 0)},
        'output': {'lines': details.get('total_lines', 0), 'errors': details.get('error_lines', 0)},
        'load': {},
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': note
    }