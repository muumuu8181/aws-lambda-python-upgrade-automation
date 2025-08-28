"""
DynamoDB書き込みLambda - Step Functions 2用
処理済みJSONデータをDynamoDBテーブルに書き込み
"""
import json
import boto3
from datetime import datetime
from typing import Dict, Any, List
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """DynamoDB書き込みメイン関数"""
    print(f"DynamoDB書き込み開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id')
        processed_items = event.get('processed_items', [])
        table_name = event.get('table_name', 'json-processing-table')
        
        if not processed_items:
            return {
                'statusCode': 400,
                'batch_id': batch_id,
                'success': False,
                'error': '処理対象データがありません',
                'evidence': create_evidence(batch_id, 'dynamodb_write', False, 
                                          {'error': '処理対象データなし'})
            }
        
        # DynamoDBテーブル取得
        table = dynamodb.Table(table_name)
        
        # バッチ書き込み実行
        success_count = 0
        failed_items = []
        
        # 25件ずつバッチ処理（DynamoDBの制限）
        for i in range(0, len(processed_items), 25):
            batch_items = processed_items[i:i+25]
            
            with table.batch_writer() as batch:
                for item in batch_items:
                    try:
                        # DynamoDB用データ形式に調整
                        dynamodb_item = {
                            'id': item['id'],
                            'batch_id': item['batch_id'],
                            'timestamp': item['timestamp'],
                            'name': item['data'].get('name', ''),
                            'category': item['data'].get('category', ''),
                            'description': item['data'].get('description', ''),
                            'metadata': json.dumps(item['data']),
                            'processed_at': item['processed_at'],
                            'ttl': int((datetime.now().timestamp()) + (30 * 24 * 60 * 60))  # 30日後
                        }
                        
                        batch.put_item(Item=dynamodb_item)
                        success_count += 1
                        
                    except Exception as e:
                        failed_items.append({
                            'item': item,
                            'error': str(e)
                        })
                        print(f"アイテム書き込みエラー: {e}")
        
        # 結果判定
        total_items = len(processed_items)
        success_rate = success_count / total_items if total_items > 0 else 0
        overall_success = success_rate >= 0.9  # 90%以上成功で全体成功とみなす
        
        result = {
            'statusCode': 200 if overall_success else 206,
            'batch_id': batch_id,
            'success': overall_success,
            'total_items': total_items,
            'success_count': success_count,
            'failed_count': len(failed_items),
            'table_name': table_name,
            'evidence': create_evidence(batch_id, 'dynamodb_write', overall_success, {
                'input_count': total_items,
                'success_count': success_count,
                'failed_count': len(failed_items),
                'table_name': table_name
            })
        }
        
        if failed_items:
            result['failed_items'] = failed_items[:5]  # 最初の5件のみ
        
        print(f"DynamoDB書き込み完了: {success_count}/{total_items}件成功")
        return result
        
    except Exception as e:
        error_msg = f"DynamoDB書き込みエラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, 'dynamodb_write', False, 
                                      {'error': error_msg})
        }

def create_evidence(batch_id: str, step: str, success: bool, details: Dict[str, Any]) -> Dict[str, Any]:
    """エビデンス作成"""
    note = f"DynamoDB書き込み{'成功' if success else '失敗'}"
    if details.get('success_count') is not None:
        note += f": {details['success_count']}/{details.get('input_count', 0)}件"
    if details.get('table_name'):
        note += f" → {details['table_name']}"
    
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'json-to-dynamodb-pipeline',
        'step': step,
        'input': {'items': details.get('input_count', 0)},
        'output': {'items': details.get('success_count', 0)},
        'load': {
            'table': details.get('table_name', ''),
            'inserted_rows': details.get('success_count', 0),
            'dropped_rows': details.get('failed_count', 0),
            'reason': 'DynamoDB batch write operation'
        },
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': note
    }