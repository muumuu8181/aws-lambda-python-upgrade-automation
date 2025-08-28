# FastAPI GUI for AWS ETL Evidence System

Step Functions パイプライン実行・監視のためのWeb GUI

## 🚀 起動方法

### 1. 依存関係インストール
```bash
cd gui/
pip install -r requirements.txt
```

### 2. AWS認証設定
```bash
# AWS CLI設定済みの場合は不要
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="your-region"
```

### 3. 設定更新
`app.py`の`STEP_FUNCTIONS`でARNを実際のものに更新:
```python
STEP_FUNCTIONS = {
    "sf1": {
        "arn": "arn:aws:states:your-region:your-account:stateMachine:...",
        # ...
    }
}
```

### 4. サーバー起動
```bash
python app.py
# または
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

### 5. ブラウザでアクセス
```
http://localhost:8000
```

## 💡 機能

### ✅ パイプライン選択・実行
- チェックボックスでStep Functions選択
- カスタムバッチID設定
- 一括実行・個別実行

### 📊 リアルタイム監視
- 実行状況リアルタイム表示
- 成功/失敗/実行中ステータス
- 実行時間・エラー詳細

### 🎨 UI/UX
- レスポンシブデザイン
- モダンなグラデーション
- 直感的な操作性

## 🔧 カスタマイズ

### Step Functions追加
`app.py`の`STEP_FUNCTIONS`に新しい定義を追加:
```python
"sf4": {
    "name": "新パイプライン",
    "arn": "arn:aws:states:...",
    "description": "説明",
    "sample_input": {...}
}
```

### スタイル変更
`static/style.css`を編集してデザインカスタマイズ

### 機能拡張
- S3証跡ファイル表示
- CloudWatch Logs統合
- Slack/メール通知

## 🐳 Docker化（オプション）

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

**作成者**: Claude AI Assistant  
**対応ブラウザ**: Chrome, Firefox, Safari, Edge