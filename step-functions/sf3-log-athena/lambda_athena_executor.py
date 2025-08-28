"""
Athena クエリ実行Lambda - Step Functions 3用
作成されたテーブルに対してAthenaクエリを実行し、結果を集計
"""
import json
import boto3
import time
from datetime import datetime
from typing import Dict, Any, List

athena = boto3.client('athena')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """Athena クエリ実行メイン関数"""
    print(f"Athena クエリ実行開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id')
        database_name = event.get('database_name', 'log_analysis_db')
        tables_created = event.get('tables_created', [])
        query_output_location = event.get('query_output_location', 's3://log-processing-dev-results/')
        
        if not tables_created:
            return {
                'statusCode': 400,
                'batch_id': batch_id,
                'success': False,
                'error': '対象テーブルがありません',
                'evidence': create_evidence(batch_id, 'athena_query', False, {'error': 'テーブルなし'})
            }
        
        # クエリ実行結果
        query_results = []
        total_rows_analyzed = 0
        
        for table_name in tables_created[:3]:  # 最大3テーブル処理
            try:
                # テーブル統計クエリ実行
                query_result = execute_table_analysis_query(
                    database_name, table_name, query_output_location
                )
                
                if query_result['success']:
                    query_results.append({
                        'table': table_name,
                        'row_count': query_result['row_count'],
                        'error_count': query_result['error_count'],
                        'sample_data': query_result['sample_data']
                    })
                    total_rows_analyzed += query_result['row_count']
                else:
                    query_results.append({
                        'table': table_name,
                        'error': query_result['error']
                    })
                    
            except Exception as e:
                print(f"テーブル分析エラー {table_name}: {e}")
                query_results.append({
                    'table': table_name,
                    'error': str(e)
                })
        
        # ログ分析サマリー生成
        log_summary = generate_log_summary(query_results)
        
        # 全体成功判定
        successful_queries = len([r for r in query_results if 'error' not in r])
        overall_success = successful_queries > 0
        
        result = {
            'statusCode': 200 if overall_success else 206,
            'batch_id': batch_id,
            'success': overall_success,
            'database_name': database_name,
            'query_results': query_results,
            'log_summary': log_summary,
            'total_rows_analyzed': total_rows_analyzed,
            'successful_queries': successful_queries,
            'evidence': create_evidence(batch_id, 'athena_query', overall_success, {
                'database': database_name,
                'tables_processed': len(query_results),
                'successful_queries': successful_queries,
                'total_rows': total_rows_analyzed
            })
        }
        
        print(f"Athena クエリ実行完了: {successful_queries}/{len(tables_created)}テーブル成功")
        return result
        
    except Exception as e:
        error_msg = f"Athena クエリ実行エラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, 'athena_query', False, {'error': error_msg})
        }

def execute_table_analysis_query(database: str, table: str, output_location: str) -> Dict[str, Any]:
    """テーブル分析クエリ実行"""
    try:
        # 行数カウントクエリ
        count_query = f"SELECT COUNT(*) as row_count FROM {database}.{table}"
        
        count_result = execute_athena_query(count_query, database, output_location)
        if not count_result['success']:
            return {'success': False, 'error': count_result['error']}
        
        row_count = count_result['data'][0][0] if count_result['data'] else 0
        
        # エラーログ数カウント（ログテーブルの場合）
        error_query = f"""
        SELECT COUNT(*) as error_count 
        FROM {database}.{table} 
        WHERE UPPER(col0) LIKE '%ERROR%' OR UPPER(col0) LIKE '%WARN%'
        """
        
        error_result = execute_athena_query(error_query, database, output_location, ignore_errors=True)
        error_count = error_result['data'][0][0] if error_result.get('data') else 0
        
        # サンプルデータ取得
        sample_query = f"SELECT * FROM {database}.{table} LIMIT 5"
        sample_result = execute_athena_query(sample_query, database, output_location)
        
        return {
            'success': True,
            'row_count': row_count,
            'error_count': error_count,
            'sample_data': sample_result.get('data', [])[:5]
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def execute_athena_query(query: str, database: str, output_location: str, ignore_errors: bool = False) -> Dict[str, Any]:
    """Athenaクエリ実行"""
    try:
        # クエリ実行
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # 実行完了待機
        max_wait = 60  # 60秒
        waited = 0
        
        while waited < max_wait:
            status_response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                # 結果取得
                result_response = athena.get_query_results(QueryExecutionId=query_execution_id)
                
                data = []
                for row in result_response['ResultSet']['Rows'][1:]:  # ヘッダー除外
                    row_data = [col.get('VarCharValue', '') for col in row['Data']]
                    # 数値変換試行
                    converted_row = []
                    for value in row_data:
                        try:
                            converted_row.append(int(value))
                        except:
                            converted_row.append(value)
                    data.append(converted_row)
                
                return {'success': True, 'data': data}
                
            elif status == 'FAILED':
                error_msg = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                if ignore_errors:
                    return {'success': False, 'error': error_msg, 'data': []}
                return {'success': False, 'error': error_msg}
            
            time.sleep(5)
            waited += 5
        
        return {'success': False, 'error': 'Query timeout'}
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def generate_log_summary(query_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """ログ分析サマリー生成"""
    summary = {
        'total_tables': len(query_results),
        'successful_tables': len([r for r in query_results if 'error' not in r]),
        'total_log_entries': 0,
        'total_error_entries': 0,
        'error_rate': 0.0
    }
    
    for result in query_results:
        if 'error' not in result:
            summary['total_log_entries'] += result.get('row_count', 0)
            summary['total_error_entries'] += result.get('error_count', 0)
    
    if summary['total_log_entries'] > 0:
        summary['error_rate'] = summary['total_error_entries'] / summary['total_log_entries']
    
    return summary

def create_evidence(batch_id: str, step: str, success: bool, details: Dict[str, Any]) -> Dict[str, Any]:
    """エビデンス作成"""
    note = f"Athena分析{'成功' if success else '失敗'}"
    if details.get('successful_queries') is not None:
        note += f": {details['successful_queries']}/{details.get('tables_processed', 0)}テーブル"
    if details.get('total_rows'):
        note += f", {details['total_rows']}行分析"
    
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'log-aggregation-athena-pipeline',
        'step': step,
        'input': {'database': details.get('database', ''), 'tables': details.get('tables_processed', 0)},
        'output': {'analyzed_rows': details.get('total_rows', 0), 'successful_queries': details.get('successful_queries', 0)},
        'load': {
            'table': 'athena_analysis_results',
            'inserted_rows': details.get('total_rows', 0),
            'dropped_rows': 0,
            'reason': 'Athena log analysis completed'
        },
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': note
    }