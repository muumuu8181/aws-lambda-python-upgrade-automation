"""
JSON処理完了Lambda - Step Functions 2用
JSON→DynamoDBパイプラインの最終処理と統計情報生成
"""
import json
import boto3
from datetime import datetime
from typing import Dict, Any, List

def lambda_handler(event, context):
    """JSON処理完了メイン関数"""
    print(f"JSON処理完了開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id')
        preprocess_result = event.get('preprocess_result', {})
        dynamodb_result = event.get('dynamodb_result', {})
        status = event.get('status', 'UNKNOWN')
        
        # 統計情報集計
        statistics = calculate_statistics(preprocess_result, dynamodb_result)
        
        # 全体成功判定
        overall_success = status == 'SUCCESS'
        
        # エビデンス生成
        evidence = create_evidence(batch_id, overall_success, statistics, status)
        
        # 最終結果
        result = {
            'statusCode': 200,
            'batch_id': batch_id,
            'overall_success': overall_success,
            'status': status,
            'summary': {
                'batch_id': batch_id,
                'status': status,
                'statistics': statistics,
                'failures': get_failures(preprocess_result, dynamodb_result),
                'completed_at': datetime.now().isoformat()
            },
            'evidence': evidence
        }
        
        print(f"JSON処理完了: {batch_id} - {status}")
        return result
        
    except Exception as e:
        error_msg = f"JSON処理完了エラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'overall_success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, False, {}, 'ERROR', error_msg)
        }

def calculate_statistics(preprocess_result: Dict[str, Any], dynamodb_result: Dict[str, Any]) -> Dict[str, Any]:
    """統計情報計算"""
    stats = {
        'input_items': 0,
        'preprocessed_items': 0,
        'dynamodb_success': 0,
        'dynamodb_failed': 0,
        'success_rate': 0.0
    }
    
    if preprocess_result:
        stats['input_items'] = len(preprocess_result.get('processed_items', []))
        stats['preprocessed_items'] = preprocess_result.get('item_count', 0)
    
    if dynamodb_result:
        stats['dynamodb_success'] = dynamodb_result.get('success_count', 0)
        stats['dynamodb_failed'] = dynamodb_result.get('failed_count', 0)
    
    total_processed = stats['preprocessed_items']
    if total_processed > 0:
        stats['success_rate'] = stats['dynamodb_success'] / total_processed
    
    return stats

def get_failures(preprocess_result, dynamodb_result):
    """失敗情報収集"""
    failures = []
    
    # 前処理失敗
    if preprocess_result and not preprocess_result.get('success', True):
        failures.append({
            'step': 'json_preprocess',
            'error': preprocess_result.get('error', 'Unknown error'),
            'details': preprocess_result
        })
    
    # DynamoDB書き込み失敗
    if dynamodb_result and dynamodb_result.get('failed_items'):
        for failed_item in dynamodb_result['failed_items'][:3]:  # 最初の3件
            failures.append({
                'step': 'dynamodb_write',
                'error': failed_item.get('error', 'Write failed'),
                'details': failed_item
            })
    
    return failures

def create_evidence(batch_id: str, success: bool, statistics: Dict[str, Any], status: str, error: str = None) -> Dict[str, Any]:
    """エビデンス作成"""
    note_parts = [f"JSON処理完了: {status}"]
    
    if statistics:
        success_count = statistics.get('dynamodb_success', 0)
        failed_count = statistics.get('dynamodb_failed', 0)
        note_parts.append(f"{success_count}件成功, {failed_count}件失敗")
    
    if error:
        note_parts.append(f"エラー: {error}")
    
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'json-to-dynamodb-pipeline',
        'step': 'finalize',
        'input': {
            'total_items': statistics.get('input_items', 0),
            'batch_id': batch_id
        },
        'output': {
            'successful_writes': statistics.get('dynamodb_success', 0),
            'failed_writes': statistics.get('dynamodb_failed', 0),
            'success_rate': statistics.get('success_rate', 0)
        },
        'load': {
            'table': 'consolidated_summary',
            'inserted_rows': statistics.get('dynamodb_success', 0),
            'dropped_rows': statistics.get('dynamodb_failed', 0),
            'reason': f"JSON処理完了: {status}"
        },
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': ' | '.join(note_parts)
    }