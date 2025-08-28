"""
Glue Crawler実行Lambda - Step Functions 3用
ログデータをGlue Crawlerでスキャンし、Athenaテーブル作成
"""
import json
import boto3
import time
from datetime import datetime
from typing import Dict, Any

glue = boto3.client('glue')

def lambda_handler(event, context):
    """Glue Crawler実行メイン関数"""
    print(f"Glue Crawler実行開始: {json.dumps(event, default=str)}")
    
    try:
        batch_id = event.get('batch_id')
        crawler_name = event.get('crawler_name', 'log-processing-crawler')
        database_name = event.get('database_name', 'log_analysis_db')
        s3_target_path = event.get('s3_target_path', 's3://log-processing-dev-source/application-logs/')
        
        # Crawlerの存在確認・作成
        crawler_exists = check_crawler_exists(crawler_name)
        if not crawler_exists:
            create_crawler(crawler_name, database_name, s3_target_path)
            print(f"新しいCrawlerを作成: {crawler_name}")
        
        # Crawler実行
        try:
            glue.start_crawler(Name=crawler_name)
            print(f"Crawler開始: {crawler_name}")
        except glue.exceptions.CrawlerRunningException:
            print(f"Crawler実行中のため待機: {crawler_name}")
        
        # 実行状況監視（最大5分）
        max_wait_time = 300  # 5分
        wait_interval = 30   # 30秒間隔
        waited_time = 0
        
        while waited_time < max_wait_time:
            crawler_status = get_crawler_status(crawler_name)
            print(f"Crawler状況: {crawler_status['state']}")
            
            if crawler_status['state'] == 'READY':
                # 実行完了
                tables_created = get_created_tables(database_name)
                
                result = {
                    'statusCode': 200,
                    'batch_id': batch_id,
                    'success': True,
                    'crawler_name': crawler_name,
                    'database_name': database_name,
                    'execution_time': waited_time,
                    'tables_created': tables_created,
                    'crawler_stats': crawler_status.get('lastCrawl', {}),
                    'evidence': create_evidence(batch_id, 'glue_crawler', True, {
                        'crawler_name': crawler_name,
                        'database': database_name,
                        'tables_created': len(tables_created),
                        'execution_time': waited_time
                    })
                }
                
                print(f"Crawler完了: {len(tables_created)}テーブル作成")
                return result
                
            elif crawler_status['state'] == 'FAILED':
                # 実行失敗
                error_msg = crawler_status.get('lastCrawl', {}).get('errorMessage', 'Unknown error')
                return {
                    'statusCode': 500,
                    'batch_id': batch_id,
                    'success': False,
                    'error': f'Crawler実行失敗: {error_msg}',
                    'evidence': create_evidence(batch_id, 'glue_crawler', False, {
                        'error': error_msg,
                        'crawler_name': crawler_name
                    })
                }
            
            # 待機
            time.sleep(wait_interval)
            waited_time += wait_interval
        
        # タイムアウト
        return {
            'statusCode': 408,
            'batch_id': batch_id,
            'success': False,
            'error': f'Crawler実行タイムアウト: {max_wait_time}秒',
            'evidence': create_evidence(batch_id, 'glue_crawler', False, {
                'error': 'タイムアウト',
                'waited_time': waited_time
            })
        }
        
    except Exception as e:
        error_msg = f"Glue Crawler実行エラー: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'success': False,
            'error': error_msg,
            'evidence': create_evidence(batch_id, 'glue_crawler', False, {'error': error_msg})
        }

def check_crawler_exists(crawler_name: str) -> bool:
    """Crawler存在確認"""
    try:
        glue.get_crawler(Name=crawler_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

def create_crawler(crawler_name: str, database_name: str, s3_path: str):
    """新しいCrawler作成"""
    # データベース作成（存在しない場合）
    try:
        glue.create_database(DatabaseInput={'Name': database_name})
    except glue.exceptions.AlreadyExistsException:
        pass  # 既に存在する場合は無視
    
    # Crawler作成
    crawler_config = {
        'Name': crawler_name,
        'Role': 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/service-role/AWSGlueServiceRole-logs',
        'DatabaseName': database_name,
        'Description': 'Log files crawler for Athena analysis',
        'Targets': {
            'S3Targets': [
                {
                    'Path': s3_path
                }
            ]
        },
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        }
    }
    
    glue.create_crawler(**crawler_config)

def get_crawler_status(crawler_name: str) -> Dict[str, Any]:
    """Crawler状況取得"""
    response = glue.get_crawler(Name=crawler_name)
    return response['Crawler']

def get_created_tables(database_name: str) -> List[str]:
    """作成されたテーブル一覧取得"""
    try:
        response = glue.get_tables(DatabaseName=database_name)
        return [table['Name'] for table in response['TableList']]
    except:
        return []

def create_evidence(batch_id: str, step: str, success: bool, details: Dict[str, Any]) -> Dict[str, Any]:
    """エビデンス作成"""
    note = f"Glue Crawler{'成功' if success else '失敗'}"
    if details.get('tables_created') is not None:
        note += f": {details['tables_created']}テーブル作成"
    if details.get('crawler_name'):
        note += f" ({details['crawler_name']})"
    
    return {
        'batch_id': batch_id,
        'test_id': '',
        'flow': 'log-aggregation-athena-pipeline',
        'step': step,
        'input': {'crawler': details.get('crawler_name', ''), 'database': details.get('database', '')},
        'output': {'tables': details.get('tables_created', 0)},
        'load': {
            'table': f"athena_tables_{details.get('tables_created', 0)}",
            'inserted_rows': details.get('tables_created', 0),
            'dropped_rows': 0,
            'reason': 'Glue Crawler table creation'
        },
        'ok': success,
        'ts': datetime.now().isoformat(),
        'note': note
    }