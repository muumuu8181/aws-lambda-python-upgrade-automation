#!/usr/bin/env python3
"""改善版HTMLレポート生成テスト"""

import json
import sys
import os
from datetime import datetime

# monitoring_lambdaから関数をインポート
sys.path.append(os.path.dirname(__file__))
from monitoring_lambda import generate_html_report, get_component_type, get_file_type

# サンプルデータ（最新実行の結果を模擬）
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
    "generated_at": datetime.now().isoformat()
}

# 複数実行リスト（サンプル）
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
    html_content = generate_html_report(
        batch_id=sample_summary["batch_id"],
        summary=sample_summary,
        execution_list=execution_list
    )
    
    # HTMLファイル保存
    output_path = "improved_report.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ 改善版HTMLレポートを生成しました: {output_path}")
    print(f"📊 バッチID: {sample_summary['batch_id']}")
    print(f"📈 処理ステップ: {len(sample_summary['steps'])}個")
    print(f"🔄 実行履歴: {len(execution_list)}件")
    
    # ブラウザで開く
    print("\n🌐 ブラウザで確認:")
    print(f"file:///{os.path.abspath(output_path).replace(os.sep, '/')}")