"""
汎用監視・集約Lambda
CloudWatch Logsサブスクリプションから証跡を収集し、JSON+HTMLレポートを生成
"""
import os
import json
import gzip
import base64
import time
import re
import boto3
from datetime import datetime
from typing import Dict, List, Any

s3 = boto3.client('s3')
logs = boto3.client('logs')

# 環境変数
EVIDENCE_BUCKET = os.environ.get('EVIDENCE_BUCKET', 'etl-observer-dev-evidence')
ENABLED = os.environ.get('ENABLED', 'true').lower() == 'true'
SAVE_RAW_LOGS = os.environ.get('SAVE_RAW_LOGS', 'false').lower() == 'true'

def safe_filename(s: str) -> str:
    """ファイル名として安全な文字列に変換"""
    return re.sub(r'[^0-9A-Za-z._/-]+', '_', s or 'unknown')

def iter_keys(bucket, prefix):
    """S3のキー一覧をページングで取得"""
    token = None
    while True:
        kw = {'Bucket': bucket, 'Prefix': prefix}
        if token: kw['ContinuationToken'] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get('Contents', []):
            yield o['Key']
        if not resp.get('IsTruncated'):
            break
        token = resp.get('NextContinuationToken')

def save_to_s3(key: str, body: str, content_type: str = 'application/json'):
    """S3にファイル保存"""
    try:
        s3.put_object(
            Bucket=EVIDENCE_BUCKET,
            Key=key,
            Body=body.encode('utf-8'),
            ContentType=content_type
        )
        print(f"Saved to s3://{EVIDENCE_BUCKET}/{key}")
    except Exception as e:
        print(f"Error saving to S3: {e}")

def get_component_type(step_name: str) -> str:
    """ステップ名からコンポーネント種別を判定"""
    component_map = {
        'prevalidate': 'Lambda関数',
        'glue_convert': 'AWS Glue Job', 
        'redshift_load': 'Lambda関数',
        'finalize': 'Lambda関数',
        'monitoring': 'Lambda関数'
    }
    return component_map.get(step_name, 'その他のAWSサービス')

def get_file_type(file_path: str) -> str:
    """ファイルパスから種別を判定"""
    if '.csv' in file_path: return 'CSVファイル'
    if '.parquet' in file_path: return 'Parquetファイル'
    if '.json' in file_path: return 'JSONファイル'
    return 'ファイル'

def generate_html_report(batch_id: str, summary: Dict[str, Any], execution_list: List[Dict] = None) -> str:
    """改善版HTMLレポート生成"""
    # フロー切り替えセクションを生成
    flow_selector = ""
    if execution_list and len(execution_list) > 1:
        flow_selector = "<div class='section'><h2>🔄 実行フロー切り替え</h2><select id='flowSelector' onchange='switchFlow()'>"
        for i, exec_info in enumerate(execution_list):
            selected = "selected" if exec_info.get('current') else ""
            flow_selector += f"<option value='{exec_info['executionArn']}' {selected}>実行 {i+1}: {exec_info['startDate']} - {exec_info['status']}</option>"
        flow_selector += "</select></div>"
    
    html_template = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="utf-8">
    <title>ETL処理エビデンスレポート - {batch_id}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px; margin-bottom: 30px; }}
        .status-ok {{ color: #28a745; font-weight: bold; }}
        .status-error {{ color: #dc3545; font-weight: bold; }}
        .section {{ margin: 30px 0; padding: 20px; border-left: 4px solid #667eea; background: #f8f9fa; }}
        .section h2 {{ margin-top: 0; color: #333; }}
        .description {{ background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 15px; font-style: italic; }}
        pre {{ background: #f5f5f5; padding: 15px; overflow-x: auto; border-radius: 5px; border: 1px solid #ddd; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #6c757d; color: white; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .metric-good {{ background-color: #d4edda; }}
        .metric-warning {{ background-color: #fff3cd; }}
        .component-badge {{ background: #17a2b8; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; }}
        select {{ padding: 8px; font-size: 14px; border-radius: 4px; border: 1px solid #ccc; }}
    </style>
    <script>
        function switchFlow() {{
            const selector = document.getElementById('flowSelector');
            const selectedArn = selector.value;
            // 実際の実装では、選択されたARNに基づいて新しいレポートをロード
            alert('実行ARN: ' + selectedArn + ' のレポートに切り替えます');
        }}
    </script>
</head>
<body>
    <div class="container">
        {flow_selector}
        
        <div class="header">
            <h1>🔍 ETL処理エビデンスレポート</h1>
            <h2>バッチID: {batch_id}</h2>
            <p><strong>処理状況:</strong> <span class="status-{summary.get('status', 'unknown').lower()}">{summary.get('status', 'UNKNOWN')}</span></p>
            <p><strong>開始時刻:</strong> {summary.get('started', 'N/A')}</p>
            <p><strong>終了時刻:</strong> {summary.get('ended', 'N/A')}</p>
            <p><strong>レポート生成:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <div class="section">
            <div class="description">
                📊 このセクションでは、ETL処理で扱ったデータの件数と種類を表示します。期待値との比較も含まれます。
            </div>
            <h2>データ処理統計</h2>
            <table>
                <tr><th>項目</th><th>ファイル種別</th><th>件数</th><th>期待値との比較</th></tr>"""

    # データ件数の詳細を追加
    counts = summary.get('counts', {})
    input_files = counts.get('input_files', 0)
    input_rows = counts.get('input_rows', 0) 
    output_files = counts.get('output_files', 0)
    output_rows = counts.get('output_rows', 0)
    redshift_loaded = counts.get('redshift_loaded', 0)
    
    # 期待値との比較
    input_comparison = "✅ 正常" if input_files > 0 else "⚠️ ファイルなし"
    output_comparison = "✅ 正常" if input_rows == output_rows else f"⚠️ 差異あり ({input_rows-output_rows}行の差)"
    redshift_comparison = "✅ 全データ正常ロード" if redshift_loaded == output_rows else f"⚠️ ロード失敗あり ({output_rows-redshift_loaded}行未ロード)"
    
    html_template += f"""
                <tr class="metric-good"><td>入力ファイル</td><td>CSVファイル</td><td>{input_files}</td><td>{input_comparison}</td></tr>
                <tr><td>入力データ行数</td><td>CSVレコード</td><td>{input_rows}</td><td>ベースライン</td></tr>
                <tr><td>出力ファイル</td><td>Parquetファイル</td><td>{output_files}</td><td>{output_comparison}</td></tr>
                <tr><td>出力データ行数</td><td>Parquetレコード</td><td>{output_rows}</td><td>変換結果</td></tr>
                <tr class="{'metric-good' if redshift_loaded == output_rows else 'metric-warning'}"><td>Redshiftロード行数</td><td>テーブルレコード</td><td>{redshift_loaded}</td><td>{redshift_comparison}</td></tr>
                <tr><td>失敗ステップ</td><td>エラー</td><td>{len(summary.get('failures', []))}</td><td>{'0で正常' if len(summary.get('failures', [])) == 0 else 'エラーあり'}</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="description">
                🔧 このセクションでは、ETLパイプラインの各ステップ（前処理、変換、ロード）の実行結果を表示します。
            </div>
            <h2>処理ステップ詳細</h2>
            <table>
                <tr><th>ステップ名</th><th>コンポーネント</th><th>ステータス</th><th>入力</th><th>出力</th><th>詳細情報</th></tr>"""

    # 各ステップの詳細を表示
    for step in summary.get('steps', []):
        step_name = step.get('step', 'N/A')
        component_type = get_component_type(step_name)
        status_class = 'status-ok' if step.get('ok') else 'status-error'
        status_text = '成功' if step.get('ok') else '失敗'
        
        input_info = step.get('input', {})
        output_info = step.get('output', {})
        
        input_s3 = input_info.get('s3', 'N/A')
        output_s3 = output_info.get('s3', 'N/A') 
        
        # ステップ名を日本語化
        step_name_jp = {
            'prevalidate': '事前検証',
            'glue_convert': 'Glue変換', 
            'redshift_load': 'Redshiftロード',
            'finalize': '終了処理',
            'monitoring': '監視処理'
        }.get(step_name, step_name)
        
        html_template += f"""
                <tr>
                    <td><strong>{step_name_jp}</strong><br><small>({step_name})</small></td>
                    <td><span class="component-badge">{component_type}</span></td>
                    <td class="{status_class}">{status_text}</td>
                    <td>{input_s3}<br><small>行数: {input_info.get('rows', 'N/A')}</small></td>
                    <td>{output_s3}<br><small>行数: {output_info.get('rows', 'N/A')}</small></td>
                    <td>{step.get('note', '')}</td>
                </tr>"""

    html_template += """
            </table>
        </div>

        <div class="section">
            <div class="description">
                ⚠️ このセクションでは、処理中に発生したエラーや失敗を表示します。
            </div>
            <h2>エラー情報</h2>"""
    
    if summary.get('failures'):
        html_template += "<table><tr><th>ステップ名</th><th>エラー種別</th><th>詳細情報</th></tr>"
        for failure in summary.get('failures', []):
            html_template += f"""
            <tr>
                <td>{failure.get('step', 'N/A')}</td>
                <td style="color: #dc3545; font-weight: bold;">{failure.get('error', 'N/A')}</td>
                <td><pre>{json.dumps(failure.get('details', {}), indent=2, ensure_ascii=False)}</pre></td>
            </tr>"""
        html_template += "</table>"
    else:
        html_template += "<p style='color: #28a745; font-weight: bold;'>✅ エラーは検出されませんでした。全ステップが正常に完了しています。</p>"

    html_template += f"""
        </div>

        <div class="section">
            <div class="description">
                📁 このセクションでは、<strong>生の証跡データ</strong>を表示します。これはAWS Step Functionsの実行ログから自動収集されたJSONデータで、監査やデバッグ用として保存されています。<br>
                <strong>S3保存場所:</strong> 証跡ファイルは以下のS3パスに保存されています。
            </div>
            <h2>生証跡データ (JSON)</h2>
            <p><strong>S3フルパス:</strong> <code>s3://{EVIDENCE_BUCKET}/evidence/{batch_id}/</code></p>
            <p><strong>データ種別:</strong> Step Functions実行ログからの自動抽出情報</p>
            <p><strong>用途:</strong> システム監査、トラブルシューティング、コンプライアンス確認</p>
            <details>
                <summary>クリックしてJSONデータを表示</summary>
                <pre>{json.dumps(summary, indent=2, ensure_ascii=False)}</pre>
            </details>
        </div>
        
        <div class="section">
            <h2>📊 レポート情報</h2>
            <p><strong>レポートバージョン:</strong> v2.0 (日本語対応・詳細化版)</p>
            <p><strong>生成時刻:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} JST</p>
            <p><strong>データ収集元:</strong> AWS CloudWatch Logs, Step Functions</p>
            <p><strong>システム:</strong> ETL自動化エビデンスシステム</p>
        </div>
        
    </div>
</body>
</html>"""
    return html_template

def get_flow_type_from_s3(execution_arn: str) -> str:
    """S3からフロー設定を読み込んでフロータイプを判定"""
    try:
        # S3から設定ファイル取得
        response = s3.get_object(Bucket='etl-observer-dev-staging', Key='config/flow_mapping.json')
        config = json.loads(response['Body'].read())
        
        flow_patterns = config['flow_patterns']
        for pattern, flow_type in flow_patterns.items():
            if pattern in execution_arn:
                return flow_type
    except Exception as e:
        print(f"フロー設定ファイル読み込みエラー: {e}")
    
    return 'unknown-pipeline'

def process_step_functions_log(log_message: str, event_id: str) -> Dict[str, Any]:
    """Step Functions ログからevidence抽出"""
    try:
        log_data = json.loads(log_message)
        event_type = log_data.get('type')
        execution_arn = log_data.get('executionArn')
        state_name = log_data.get('state')
        
        # input/outputからevidenceを探す
        evidence = None
        for key in ['input', 'output']:
            if isinstance(log_data.get(key), dict):
                if 'evidence' in log_data[key]:
                    evidence = log_data[key]['evidence']
                    break
        
        if not evidence and 'evidence' in log_data:
            evidence = log_data['evidence']
            
        if evidence:
            # フロータイプを動的に設定
            flow_type = get_flow_type_from_s3(execution_arn)
            evidence.update({
                'event_type': event_type,
                'execution_arn': execution_arn,
                'state_name': state_name,
                'event_id': event_id,
                'flow': flow_type,  # 動的フロータイプ設定
                'ts': datetime.now().isoformat()
            })
            return evidence
    except Exception as e:
        print(f"Error processing Step Functions log: {e}")
    return None

def process_glue_log(log_message: str, event_id: str) -> Dict[str, Any]:
    """Glue ログからevidence抽出"""
    try:
        if log_message.startswith('EVIDENCE '):
            evidence_json = log_message[len('EVIDENCE '):]
            evidence = json.loads(evidence_json)
            if 'evidence' in evidence:
                evidence = evidence['evidence']
            evidence.update({
                'event_id': event_id,
                'ts': datetime.now().isoformat()
            })
            return evidence
    except Exception as e:
        print(f"Error processing Glue log: {e}")
    return None

def aggregate_evidences(batch_id: str) -> Dict[str, Any]:
    """batch_idに関連するevidenceファイルを集約してサマリ作成（ページング対応版）"""
    try:
        prefix = f"evidence/{batch_id}/per-step/"
        steps, failures = [], []
        counts = {'input_files':0,'input_rows':0,'output_files':0,'output_rows':0,'redshift_loaded':0}

        for key in iter_keys(EVIDENCE_BUCKET, prefix):
            file_response = s3.get_object(Bucket=EVIDENCE_BUCKET, Key=key)
            evidence = json.loads(file_response['Body'].read().decode('utf-8'))
            steps.append(evidence)
            if evidence.get('input', {}).get('rows'):
                counts['input_rows'] += evidence['input']['rows']; counts['input_files'] += 1
            if evidence.get('output', {}).get('rows'):
                counts['output_rows'] += evidence['output']['rows']; counts['output_files'] += 1
            if evidence.get('load', {}).get('inserted_rows'):
                counts['redshift_loaded'] += evidence['load']['inserted_rows']
            if not evidence.get('ok', True):
                failures.append({'step': evidence.get('step'),
                                 'error': evidence.get('error','Unknown error'),
                                 'details': evidence})
        
        return {
            'batch_id': batch_id,
            'status': 'ERROR' if failures else 'OK',
            'started': min((s.get('ts') for s in steps), default=datetime.now().isoformat()),
            'ended':   max((s.get('ts') for s in steps), default=datetime.now().isoformat()),
            'counts': counts, 'steps': steps, 'failures': failures,
            'generated_at': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error aggregating evidences: {e}")
        return {
            'batch_id': batch_id,
            'status': 'ERROR',
            'error': str(e),
            'generated_at': datetime.now().isoformat()
        }

def lambda_handler(event, context):
    """メインハンドラー（終端確定化対応版）"""
    if not ENABLED:
        print("Monitoring disabled by ENABLED=false")
        return {'ok': True, 'message': 'disabled'}

    terminal_batches = set()   # 終端検知されたバッチID
    batch_ids_seen   = set()   # 全体で見つかったバッチID

    try:
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        log_data = json.loads(gzip.decompress(compressed_payload))
        log_group = log_data.get('logGroup', '')
        
        print(f"Processing {len(log_data.get('logEvents', []))} log events from {log_group}")
        
        # 生ログ保存（オプション）
        if SAVE_RAW_LOGS:
            timestamp = str(int(time.time()))
            raw_key = f"raw-logs/{timestamp}_{safe_filename(log_group)}.json"
            save_to_s3(raw_key, json.dumps(log_data, ensure_ascii=False, indent=2))
        
        for le in log_data.get('logEvents', []):
            msg = le.get('message','')
            event_id = le.get('id')

            # 1) Glueの EVIDENCE 行
            evidence = None
            if 'EVIDENCE ' in msg:
                evidence = process_glue_log(msg, event_id)

            # 2) Step Functions のJSONログ
            is_terminal = False
            if '/aws/states/' in log_group:
                try:
                    m = json.loads(msg)
                    t = m.get('type')
                    is_terminal = t in ('ExecutionSucceeded','ExecutionFailed','ExecutionAborted','ExecutionTimedOut')
                    if not evidence:
                        evidence = process_step_functions_log(msg, event_id)
                except Exception:
                    pass

            if evidence:
                batch_id = evidence.get('batch_id') or f"B{int(time.time())}"
                batch_ids_seen.add(batch_id)
                per_step_key = f"evidence/{batch_id}/per-step/{safe_filename(evidence.get('step','unknown'))}_{event_id}.json"
                save_to_s3(per_step_key, json.dumps(evidence, ensure_ascii=False, indent=2))
                if is_terminal or evidence.get('is_terminal'):
                    terminal_batches.add(batch_id)   # 終端だけ確定集計対象に
        
        # ここで"終端だけ"集計
        for bid in terminal_batches:
            summary = aggregate_evidences(bid)
            save_to_s3(f"evidence/{bid}/summary.json", json.dumps(summary, ensure_ascii=False, indent=2))
            html = generate_html_report(bid, summary)
            save_to_s3(f"evidence/{bid}/report.html", html, 'text/html')
            print(f"Generated report for batch {bid}")
        
        return {'ok': True,
                'processed_batches': list(batch_ids_seen),
                'finalized_batches': list(terminal_batches),
                'log_group': log_group,
                'events_count': len(log_data.get('logEvents', []))}
        
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {
            'ok': False,
            'error': str(e)
        }