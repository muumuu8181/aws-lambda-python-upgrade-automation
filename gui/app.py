"""
FastAPI GUI for AWS ETL Evidence System
Step Functions 実行・監視GUI
"""
import json
import boto3
from datetime import datetime
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any
import logging
import os

# システム設定読み込み
def load_system_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'system_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

SYSTEM_CONFIG = load_system_config()

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=SYSTEM_CONFIG["system"]["name"] + " GUI", 
    version=SYSTEM_CONFIG["system"]["version"]
)

# テンプレート・静的ファイル設定
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# AWS クライアント
stepfunctions = boto3.client(
    'stepfunctions', 
    region_name=SYSTEM_CONFIG["aws"]["region"]
)

# Step Functions 設定（設定ファイルから自動生成）
def build_step_functions_config():
    config = {}
    aws_config = SYSTEM_CONFIG["aws"]
    sf_config = SYSTEM_CONFIG["step_functions"]
    s3_config = SYSTEM_CONFIG["s3_buckets"]
    
    for sf_id, sf_data in sf_config.items():
        arn = f"arn:aws:states:{aws_config['region']}:{aws_config['account_id']}:stateMachine:{sf_data['state_machine_name']}"
        
        # サンプル入力データを生成
        if sf_id == "sf1":
            sample_input = {
                "batch_id": "CSV_BATCH_001",
                "dataset": "employees", 
                "files": [f"s3://{s3_config['landing']}/employees.csv"]
            }
        elif sf_id == "sf2":
            sample_input = {
                "batch_id": "JSON_BATCH_001",
                "input_data": {"key1": "value1", "key2": "value2"}
            }
        else:  # sf3
            sample_input = {
                "batch_id": "LOG_BATCH_001",
                "log_bucket": s3_config["staging"],
                "log_prefix": "logs/"
            }
        
        config[sf_id] = {
            "name": sf_data["name"],
            "arn": arn,
            "description": sf_data["description"],
            "sample_input": sample_input
        }
    
    return config

STEP_FUNCTIONS = build_step_functions_config()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """メインページ"""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "step_functions": STEP_FUNCTIONS
    })

@app.post("/execute", response_class=JSONResponse)
async def execute_step_functions(
    selected_sfs: List[str] = Form(...),
    custom_batch_id: str = Form("")
):
    """選択されたStep Functionsを実行"""
    results = []
    
    for sf_id in selected_sfs:
        if sf_id not in STEP_FUNCTIONS:
            continue
            
        sf_config = STEP_FUNCTIONS[sf_id]
        
        # バッチIDカスタマイズ
        input_data = sf_config["sample_input"].copy()
        if custom_batch_id:
            input_data["batch_id"] = f"{custom_batch_id}_{sf_id.upper()}"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            input_data["batch_id"] = f"GUI_{timestamp}_{sf_id.upper()}"
        
        try:
            # Step Functions実行
            execution_name = f"gui-{input_data['batch_id'].lower()}"
            response = stepfunctions.start_execution(
                stateMachineArn=sf_config["arn"],
                name=execution_name,
                input=json.dumps(input_data)
            )
            
            results.append({
                "sf_id": sf_id,
                "name": sf_config["name"],
                "status": "SUCCESS",
                "execution_arn": response["executionArn"],
                "start_date": response["startDate"].isoformat(),
                "input_data": input_data
            })
            
            logger.info(f"Started {sf_id}: {response['executionArn']}")
            
        except Exception as e:
            results.append({
                "sf_id": sf_id,
                "name": sf_config["name"],
                "status": "ERROR",
                "error": str(e),
                "input_data": input_data
            })
            
            logger.error(f"Failed to start {sf_id}: {e}")
    
    return {"results": results}

@app.get("/status/{execution_arn:path}", response_class=JSONResponse)
async def get_execution_status(execution_arn: str):
    """Step Functions実行状況取得"""
    try:
        response = stepfunctions.describe_execution(executionArn=execution_arn)
        return {
            "status": "SUCCESS",
            "execution_status": response["status"],
            "start_date": response["startDate"].isoformat(),
            "stop_date": response.get("stopDate", "").isoformat() if response.get("stopDate") else None,
            "input": json.loads(response["input"]),
            "output": json.loads(response.get("output", "{}")) if response.get("output") else None,
            "error": response.get("error"),
            "cause": response.get("cause")
        }
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

@app.get("/health", response_class=JSONResponse)
async def health_check():
    """ヘルスチェック"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": SYSTEM_CONFIG["system"]["version"],
        "system": SYSTEM_CONFIG["system"]["name"]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)