"""
AWS Glue Job: CSV to Parquet 変換
証跡ログ出力機能付き
"""
import sys
import json
import logging
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # 引数取得
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'batch_id',
        'src_s3_uri',
        'dst_s3_uri',
        'dataset_name'
    ])
    
    # Spark/Glue 初期化
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    batch_id = args['batch_id']
    src_s3_uri = args['src_s3_uri']
    dst_s3_uri = args['dst_s3_uri']
    dataset_name = args.get('dataset_name', 'unknown')
    
    input_rows = 0
    output_rows = 0
    error_message = None
    success = True
    
    try:
        logger.info(f"Starting CSV to Parquet conversion: {src_s3_uri} -> {dst_s3_uri}")
        
        # CSV読み込み
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(src_s3_uri)
        input_rows = df.count()
        logger.info(f"Input rows: {input_rows}")
        
        # データ変換処理（必要に応じてここで変換ロジックを追加）
        # 例: 日付フォーマット変更、カラム名正規化、データ型変換など
        processed_df = df
        
        # Parquet出力
        processed_df.write.mode("overwrite").parquet(dst_s3_uri)
        
        # 出力件数確認
        output_df = spark.read.parquet(dst_s3_uri)
        output_rows = output_df.count()
        logger.info(f"Output rows: {output_rows}")
        
        logger.info(f"Successfully converted CSV to Parquet: {output_rows} rows")
        
    except Exception as e:
        success = False
        error_message = str(e)
        logger.error(f"Error during conversion: {e}", exc_info=True)
    
    finally:
        # 証跡ログ出力（必須）
        evidence = {
            "evidence": {
                "batch_id": batch_id,
                "test_id": "",
                "flow": "csv-to-parquet-pipeline",
                "step": "glue_convert",
                "input": {
                    "s3": src_s3_uri,
                    "rows": input_rows,
                    "dataset": dataset_name
                },
                "output": {
                    "s3": dst_s3_uri,
                    "rows": output_rows,
                    "format": "parquet"
                },
                "load": {},
                "ok": success,
                "ts": datetime.now().isoformat(),
                "note": error_message if error_message else f"Converted {input_rows} rows to {output_rows} rows"
            }
        }
        
        # この行が監視Lambdaに拾われる
        logger.info("EVIDENCE " + json.dumps(evidence, ensure_ascii=False))
        
        job.commit()
        
        if not success:
            raise Exception(f"Glue job failed: {error_message}")

if __name__ == "__main__":
    main()