"""
JSON前処理Lambda - Step Functions 2用
JSONデータを検証・変換してDynamoDB投入用に準備
"""
import json
import boto3
import uuid
from datetime import datetime
from typing import Dict, Any, List

def lambda_handler(event, context):
    """JSON前処理メイン関数"""
    print(f"JSON前処理開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id', f'JSON_{datetime.now().strftime("%Y%m%d%H%M")}')
        input_data = event.get('input_data', {})
        
        # JSONデータ検証
        validation_result = validate_json_data(input_data)
        if not validation_result['valid']:
            return {
                'statusCode': 400,
                'batch_id': batch_id,
                'success': False,
                'error': f"JSON検証失敗: {validation_result['errors']}",
                'evidence': create_evidence(batch_id, 'json_preprocess', False, 
                                          {'error': validation_result['errors']})
            }
        
        # DynamoDB用データ変換
        processed_items = []
        for item in input_data.get('items', []):
            processed_item = {
                'id': str(uuid.uuid4()),
                'batch_id': batch_id,
                'timestamp': datetime.now().isoformat(),
                'data': item,
                'processed_at': datetime.now().isoformat()
            }
            processed_items.append(processed_item)
        
        # 成功レスポンス
        result = {
            'statusCode': 200,
            'batch_id': batch_id,
            'success': True,
            'processed_items': processed_items,
            'item_count': len(processed_items),
            'evidence': create_evidence(batch_id, 'json_preprocess', True, {
                'input_count': len(input_data.get('items', [])),
                'output_count': len(processed_items)
            })
        }
        
        print(f"JSON前処理完了: {result['item_count']}件処理")
        return result
        
    except Exception as e:
        error_msg = f"JSON前処理エラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, 'json_preprocess', False, 
                                      {'error': error_msg})
        }

def validate_json_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """JSONデータ検証"""
    errors = []
    
    if not isinstance(data, dict):
        errors.append("データがDict形式ではありません")
        return {'valid': False, 'errors': errors}
    
    if 'items' not in data:
        errors.append("'items'キーが存在しません")
    elif not isinstance(data['items'], list):
        errors.append("'items'がリスト形式ではありません")
    elif len(data['items']) == 0:
        errors.append("'items'が空です")
    
    # 必須フィールドチェック
    required_fields = ['name', 'category']
    for i, item in enumerate(data.get('items', [])):
        for field in required_fields:
            if field not in item:
                errors.append(f"アイテム{i}: 必須フィールド'{field}'が存在しません")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors
    }

def create_evidence(batch_id: str, step: str, success: bool, details: Dict[str, Any]) -> Dict[str, Any]:
    """エビデンス作成"""
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'json-to-dynamodb-pipeline',
        'step': step,
        'input': details.get('input', {}),
        'output': details.get('output', {}),
        'load': {},
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': f"JSON前処理{'成功' if success else '失敗'}: {details.get('input_count', 0)}→{details.get('output_count', 0)}件"
    }