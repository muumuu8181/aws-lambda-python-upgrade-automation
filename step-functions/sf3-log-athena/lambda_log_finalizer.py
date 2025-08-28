"""
ログ処理完了Lambda - Step Functions 3用
ログ集約→Athenaパイプラインの最終処理と統計情報生成
"""
import json
import boto3
from datetime import datetime
from typing import Dict, Any, List

def lambda_handler(event, context):
    """ログ処理完了メイン関数"""
    print(f"ログ処理完了開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id')
        log_collect_result = event.get('log_collect_result', {})
        crawler_result = event.get('crawler_result', {})
        athena_result = event.get('athena_result', {})
        status = event.get('status', 'UNKNOWN')
        
        # 統計情報集計
        statistics = calculate_log_statistics(log_collect_result, crawler_result, athena_result)
        
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
                'failures': get_failures(log_collect_result, crawler_result, athena_result),
                'completed_at': datetime.now().isoformat()
            },
            'evidence': evidence
        }
        
        print(f"ログ処理完了: {batch_id} - {status}")
        return result
        
    except Exception as e:
        error_msg = f"ログ処理完了エラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'overall_success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, False, {}, 'ERROR', error_msg)
        }

def calculate_log_statistics(log_collect_result: Dict[str, Any], 
                           crawler_result: Dict[str, Any], 
                           athena_result: Dict[str, Any]) -> Dict[str, Any]:
    """ログ処理統計情報計算"""
    stats = {
        'log_files_processed': 0,
        'total_log_lines': 0,
        'error_log_lines': 0,
        'tables_created': 0,
        'athena_queries_executed': 0,
        'rows_analyzed': 0,
        'processing_success_rate': 0.0
    }
    
    # ログ収集結果
    if log_collect_result and log_collect_result.get('success'):
        aggregated_stats = log_collect_result.get('aggregated_stats', {})
        stats['log_files_processed'] = aggregated_stats.get('processed_files', 0)
        stats['total_log_lines'] = aggregated_stats.get('total_log_lines', 0)
        stats['error_log_lines'] = aggregated_stats.get('error_log_lines', 0)
    
    # Crawler結果
    if crawler_result and crawler_result.get('success'):
        stats['tables_created'] = len(crawler_result.get('tables_created', []))
    
    # Athena結果
    if athena_result and athena_result.get('success'):
        stats['athena_queries_executed'] = athena_result.get('successful_queries', 0)
        stats['rows_analyzed'] = athena_result.get('total_rows_analyzed', 0)
    
    # 成功率計算
    total_steps = 3  # log_collect, crawler, athena
    successful_steps = sum([
        1 if log_collect_result.get('success') else 0,
        1 if crawler_result.get('success') else 0,
        1 if athena_result.get('success') else 0
    ])
    stats['processing_success_rate'] = successful_steps / total_steps if total_steps > 0 else 0
    
    return stats

def get_failures(log_collect_result: Dict[str, Any], 
                crawler_result: Dict[str, Any], 
                athena_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """失敗情報収集"""
    failures = []
    
    # ログ収集失敗
    if log_collect_result and not log_collect_result.get('success'):
        failures.append({
            'step': 'log_collect',
            'error': log_collect_result.get('error', 'Log collection failed'),
            'details': log_collect_result
        })
    
    # Crawler失敗
    if crawler_result and not crawler_result.get('success'):
        failures.append({
            'step': 'glue_crawler',
            'error': crawler_result.get('error', 'Glue crawler failed'),
            'details': crawler_result
        })
    
    # Athena失敗
    if athena_result and not athena_result.get('success'):
        failures.append({
            'step': 'athena_query',
            'error': athena_result.get('error', 'Athena query failed'),
            'details': athena_result
        })
    
    return failures

def create_evidence(batch_id: str, success: bool, statistics: Dict[str, Any], status: str, error: str = None) -> Dict[str, Any]:
    """エビデンス作成"""
    note_parts = [f"ログ処理完了: {status}"]
    
    if statistics:
        files_processed = statistics.get('log_files_processed', 0)
        lines_processed = statistics.get('total_log_lines', 0)
        tables_created = statistics.get('tables_created', 0)
        
        note_parts.append(f"{files_processed}ファイル, {lines_processed}行処理")
        note_parts.append(f"{tables_created}テーブル作成")
    
    if error:
        note_parts.append(f"エラー: {error}")
    
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'log-aggregation-athena-pipeline',
        'step': 'finalize',
        'input': {
            'log_files': statistics.get('log_files_processed', 0),
            'batch_id': batch_id
        },
        'output': {
            'tables_created': statistics.get('tables_created', 0),
            'rows_analyzed': statistics.get('rows_analyzed', 0),
            'success_rate': statistics.get('processing_success_rate', 0)
        },
        'load': {
            'table': 'consolidated_log_summary',
            'inserted_rows': statistics.get('rows_analyzed', 0),
            'dropped_rows': 0,
            'reason': f"ログ処理完了: {status}"
        },
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': ' | '.join(note_parts)
    }