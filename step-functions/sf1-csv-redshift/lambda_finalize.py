"""
ETL最終処理Lambda - バッチ全体のサマリ作成
"""
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """
    バッチ処理の最終まとめ
    """
    batch_id = event.get('batch_id')
    files = event.get('files', [])
    map_results = event.get('map_results', [])
    prevalidate_result = event.get('prevalidate_result', {})
    
    # 集計処理
    total_input_files = len(files)
    successful_conversions = 0
    successful_loads = 0
    total_input_rows = 0
    total_output_rows = 0
    total_loaded_rows = 0
    failures = []
    
    try:
        for result in map_results:
            # Glue結果チェック
            if 'glue_result' in result and result['glue_result'].get('JobRunState') == 'SUCCEEDED':
                successful_conversions += 1
            else:
                failures.append({
                    'step': 'glue_convert',
                    'file': result.get('key', 'unknown'),
                    'error': result.get('glue_result', {}).get('ErrorMessage', 'Glue job failed')
                })
            
            # Redshift結果チェック
            if 'redshift_result' in result:
                redshift_payload = result['redshift_result'].get('Payload', {})
                if isinstance(redshift_payload, str):
                    redshift_payload = json.loads(redshift_payload)
                
                if redshift_payload.get('success'):
                    successful_loads += 1
                    total_loaded_rows += redshift_payload.get('inserted_rows', 0)
                else:
                    failures.append({
                        'step': 'redshift_load',
                        'file': result.get('key', 'unknown'),
                        'error': redshift_payload.get('error', 'Redshift load failed')
                    })
        
        # プリバリデーション結果から統計取得
        prevalidate_payload = prevalidate_result.get('Payload', {})
        if isinstance(prevalidate_payload, str):
            prevalidate_payload = json.loads(prevalidate_payload)
        
        validation_errors = prevalidate_payload.get('validation_errors', [])
        if validation_errors:
            for error in validation_errors:
                failures.append({
                    'step': 'prevalidate',
                    'file': 'validation',
                    'error': error
                })
        
        # 全体成功判定
        overall_success = (len(failures) == 0 and 
                          successful_conversions == total_input_files and
                          successful_loads == total_input_files)
        
        # 最終証跡情報
        evidence = {
            "batch_id": batch_id,
            "test_id": "",
            "flow": "csv-to-parquet-pipeline",
            "step": "finalize",
            "input": {
                "total_files": total_input_files,
                "batch_id": batch_id
            },
            "output": {
                "successful_conversions": successful_conversions,
                "successful_loads": successful_loads,
                "total_failures": len(failures)
            },
            "load": {
                "table": "consolidated_summary",
                "inserted_rows": total_loaded_rows,
                "dropped_rows": 0,
                "reason": f"Batch processing completed: {successful_loads}/{total_input_files} files loaded successfully"
            },
            "ok": overall_success,
            "ts": datetime.now().isoformat(),
            "note": f"Finalized batch {batch_id}: {successful_conversions} conversions, {successful_loads} loads, {len(failures)} failures"
        }
        
        # 詳細サマリ
        summary = {
            "batch_id": batch_id,
            "status": "SUCCESS" if overall_success else "PARTIAL_FAILURE",
            "statistics": {
                "total_input_files": total_input_files,
                "successful_conversions": successful_conversions,
                "successful_loads": successful_loads,
                "total_input_rows": total_input_rows,
                "total_output_rows": total_output_rows,
                "total_loaded_rows": total_loaded_rows,
                "failure_count": len(failures)
            },
            "failures": failures,
            "completed_at": datetime.now().isoformat()
        }
        
        print(f"Batch {batch_id} finalized: {successful_loads}/{total_input_files} successful")
        
        return {
            'statusCode': 200,
            'batch_id': batch_id,
            'overall_success': overall_success,
            'summary': summary,
            'evidence': evidence
        }
        
    except Exception as e:
        error_evidence = {
            "batch_id": batch_id,
            "step": "finalize",
            "ok": False,
            "error": str(e),
            "ts": datetime.now().isoformat(),
            "note": f"Failed to finalize batch {batch_id}: {str(e)}"
        }
        
        return {
            'statusCode': 500,
            'error': str(e),
            'evidence': error_evidence
        }