#!/usr/bin/env python3
"""スタンドアロン改善版HTMLレポート生成"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any

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

def generate_improved_html_report(batch_id: str, summary: Dict[str, Any], execution_list: List[Dict] = None) -> str:
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
        .status-success {{ color: #28a745; font-weight: bold; }}
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
        .component-type {{ color: #495057; font-weight: 600; font-size: 0.9em; }}
        select {{ padding: 8px; font-size: 14px; border-radius: 4px; border: 1px solid #ccc; }}
        details {{ margin: 10px 0; }}
        summary {{ cursor: pointer; font-weight: bold; padding: 10px; background: #f8f9fa; border-radius: 4px; }}
        .data-sample {{ background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; padding: 15px; margin: 10px 0; }}
        .data-sample h4 {{ margin-top: 0; color: #495057; }}
        .sample-table {{ font-size: 0.85em; }}
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
            <p><strong>処理状況:</strong> <span class="status-{summary.get('status', 'unknown').lower()}">✅ {summary.get('status', 'UNKNOWN')}</span></p>
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
    redshift_comparison = "✅ 全データ正常ロード" if redshift_loaded >= output_rows else f"⚠️ ロード失敗あり ({output_rows-redshift_loaded}行未ロード)"
    
    html_template += f"""
                <tr class="metric-good"><td>入力ファイル</td><td>CSVファイル</td><td>{input_files}</td><td>{input_comparison}</td></tr>
                <tr><td>入力データ行数</td><td>CSVレコード</td><td>{input_rows}</td><td>ベースライン</td></tr>
                <tr><td>出力ファイル</td><td>Parquetファイル</td><td>{output_files}</td><td>{output_comparison}</td></tr>
                <tr><td>出力データ行数</td><td>Parquetレコード</td><td>{output_rows}</td><td>変換結果</td></tr>
                <tr class="{'metric-good' if redshift_loaded >= output_rows else 'metric-warning'}"><td>Redshiftロード行数</td><td>テーブルレコード</td><td>{redshift_loaded}</td><td>{redshift_comparison}</td></tr>
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
                    <td><span class="component-type">{component_type}</span></td>
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
                🗂️ このセクションでは、Redshiftテーブルにロードされたデータの実際のサンプル（先頭10行）を表示します。データ品質の確認に利用できます。
            </div>
            <h2>Redshiftデータサンプル</h2>"""
    
    # Redshiftサンプルデータがある場合は表示
    redshift_sample = summary.get('redshift_sample')
    if redshift_sample and redshift_sample.get('data'):
        sample_data = redshift_sample['data']
        columns = redshift_sample.get('columns', [])
        table_name = redshift_sample.get('table', 'public.employees')
        
        html_template += f"""
            <div class="data-sample">
                <h4>📋 テーブル: {table_name} (先頭10行)</h4>
                <p><strong>総行数:</strong> {redshift_loaded}行 | <strong>サンプル行数:</strong> {len(sample_data)}行</p>
                <table class="sample-table">
                    <tr>"""
        
        # ヘッダー行
        for col in columns:
            html_template += f"<th>{col}</th>"
        html_template += "</tr>"
        
        # データ行
        for row in sample_data[:10]:  # 最大10行
            html_template += "<tr>"
            for value in row:
                display_value = str(value) if value is not None else "NULL"
                if len(display_value) > 50:  # 長い値は省略
                    display_value = display_value[:47] + "..."
                html_template += f"<td>{display_value}</td>"
            html_template += "</tr>"
        
        html_template += """
                </table>
            </div>"""
    else:
        html_template += """
            <div class="data-sample">
                <p style="color: #6c757d; font-style: italic;">
                    📝 Redshiftデータサンプルは現在収集されていません。<br>
                    <small>データサンプルを表示するには、monitoring_lambda.pyでRedshiftクエリ実行機能を有効化してください。</small>
                </p>
            </div>"""

    html_template += """
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
            <p><strong>S3フルパス:</strong> <code>s3://etl-observer-dev-evidence/evidence/{batch_id}/</code></p>
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

# サンプルデータ
sample_summary = {
    "batch_id": "B20250827100",
    "status": "SUCCESS", 
    "started": "2025-08-27T14:00:34.867000+09:00",
    "ended": "2025-08-27T14:02:54.284000+09:00",
    "counts": {
        "input_files": 1,
        "input_rows": 10,
        "output_files": 1, 
        "output_rows": 10,
        "redshift_loaded": 20  # 重複分も含む
    },
    "steps": [
        {
            "step": "prevalidate",
            "ok": True,
            "input": {"files_count": 1, "total_size_bytes": 714},
            "output": {"validated_files": 1, "errors": []},
            "note": "1ファイルのバリデーションが正常完了",
            "ts": "2025-08-27T14:00:40"
        },
        {
            "step": "glue_convert", 
            "ok": True,
            "input": {"s3": "s3://etl-observer-dev-landing/test/sample.csv", "rows": 10},
            "output": {"s3": "s3://etl-observer-dev-staging/parquet/employees/test/sample.csv", "rows": 10},
            "note": "CSV→Parquet変換成功: 10行→10行",
            "ts": "2025-08-27T14:01:45"
        },
        {
            "step": "redshift_load",
            "ok": True, 
            "input": {"s3": "s3://etl-observer-dev-staging/parquet/employees/test/sample.csv", "rows": 10},
            "output": {"rows": 20},
            "note": "Redshiftに20行正常ロード完了 (既存データ含む)",
            "ts": "2025-08-27T14:02:30"
        },
        {
            "step": "finalize",
            "ok": True,
            "input": {"total_files": 1, "batch_id": "B20250827100"}, 
            "output": {"successful_conversions": 1, "successful_loads": 1},
            "note": "バッチ処理正常終了: 1変換, 1ロード, 0失敗",
            "ts": "2025-08-27T14:02:54"
        }
    ],
    "failures": [],
    "redshift_sample": {
        "table": "public.employees",
        "columns": ["employee_id", "first_name", "last_name", "email", "age", "department", "salary", "hire_date"],
        "data": [
            [1, "John", "Doe", "john.doe@example.com", 28, "Engineering", 75000, "2023-01-15"],
            [2, "Jane", "Smith", "jane.smith@example.com", 32, "Marketing", 68000, "2022-03-10"],
            [3, "Mike", "Johnson", "mike.johnson@example.com", 45, "Sales", 82000, "2021-07-22"],
            [4, "Sarah", "Wilson", "sarah.wilson@example.com", 29, "Engineering", 77000, "2023-02-01"],
            [5, "David", "Brown", "david.brown@example.com", 38, "HR", 65000, "2020-11-30"],
            [6, "Lisa", "Davis", "lisa.davis@example.com", 26, "Marketing", 58000, "2023-06-15"],
            [7, "Tom", "Anderson", "tom.anderson@example.com", 41, "Engineering", 95000, "2019-09-08"],
            [8, "Emily", "Taylor", "emily.taylor@example.com", 33, "Sales", 71000, "2022-12-05"],
            [9, "James", "Miller", "james.miller@example.com", 31, "Engineering", 79000, "2023-04-12"],
            [10, "Susan", "Garcia", "susan.garcia@example.com", 27, "HR", 62000, "2023-08-20"]
        ]
    },
    "generated_at": datetime.now().isoformat()
}

# 複数実行リスト
execution_list = [
    {
        "executionArn": "arn:aws:states:YOUR_AWS_REGION:YOUR_AWS_ACCOUNT_ID:execution:sf-etl-observer-dev-ingest:bf983fbe-a93f-4c2b-a949-2b8ade4c7040",
        "startDate": "2025-08-27T14:00:34.867000+09:00", 
        "status": "SUCCEEDED",
        "current": True
    },
    {
        "executionArn": "arn:aws:states:YOUR_AWS_REGION:YOUR_AWS_ACCOUNT_ID:execution:sf-etl-observer-dev-ingest:eb8ae983-4ffe-4cfc-9798-9ebeb3403eee",
        "startDate": "2025-08-27T11:08:55.983000+09:00",
        "status": "SUCCEEDED", 
        "current": False
    }
]

if __name__ == "__main__":
    print("🚀 改善版HTMLレポート生成開始...")
    
    # 改善版HTMLレポート生成
    html_content = generate_improved_html_report(
        batch_id=sample_summary["batch_id"],
        summary=sample_summary,
        execution_list=execution_list
    )
    
    # HTMLファイル保存
    output_path = "improved_report_v2.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ 改善版HTMLレポートを生成しました: {output_path}")
    print(f"📊 バッチID: {sample_summary['batch_id']}")
    print(f"📈 処理ステップ: {len(sample_summary['steps'])}個")
    print(f"🔄 実行履歴: {len(execution_list)}件")
    
    # パス表示
    full_path = os.path.abspath(output_path).replace('\\', '/')
    print(f"\n🌐 ブラウザで開くパス:")
    print(f"file:///{full_path}")