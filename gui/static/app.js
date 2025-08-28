// AWS ETL Evidence System GUI JavaScript

class ETLSystemGUI {
    constructor() {
        this.executionResults = [];
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.showDateTime();
    }

    setupEventListeners() {
        // フォーム送信
        const form = document.getElementById('execution-form');
        form.addEventListener('submit', (e) => this.handleFormSubmit(e));

        // 全選択/全解除
        document.getElementById('select-all').addEventListener('click', () => this.selectAll());
        document.getElementById('clear-all').addEventListener('click', () => this.clearAll());

        // 全ログコピー（実行ログ + 監視ログの統合）
        document.getElementById('copy-all-logs').addEventListener('click', () => this.copyAllLogs());

        // 状況更新
        document.getElementById('refresh-status').addEventListener('click', () => this.refreshStatus());
    }

    showDateTime() {
        // 現在時刻を表示（オプション）
        const now = new Date();
        console.log('GUI初期化完了:', now.toISOString());
    }

    async handleFormSubmit(e) {
        e.preventDefault();

        const formData = new FormData(e.target);
        const selectedSfs = formData.getAll('selected_sfs');
        
        if (selectedSfs.length === 0) {
            alert('実行するパイプラインを選択してください。');
            return;
        }

        // 実行ボタンを無効化
        const submitBtn = e.target.querySelector('button[type="submit"]');
        const originalText = submitBtn.innerHTML;
        submitBtn.innerHTML = '🔄 実行中...';
        submitBtn.disabled = true;

        try {
            const response = await fetch('/execute', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            this.displayResults(result.results);
            this.executionResults = result.results;
            
            // ログコピーセクションを表示
            document.getElementById('log-actions-section').style.display = 'block';
            
            // 状況監視セクションを表示
            if (this.executionResults.some(r => r.status === 'SUCCESS')) {
                document.getElementById('status-section').style.display = 'block';
                setTimeout(() => this.refreshStatus(), 1000);
            }

        } catch (error) {
            console.error('実行エラー:', error);
            alert(`実行エラーが発生しました: ${error.message}`);
        } finally {
            // ボタンを復元
            submitBtn.innerHTML = originalText;
            submitBtn.disabled = false;
        }
    }

    displayResults(results) {
        const resultsSection = document.getElementById('results');
        const container = document.getElementById('results-container');
        
        container.innerHTML = '';
        
        results.forEach(result => {
            const resultDiv = document.createElement('div');
            resultDiv.className = `result-item ${result.status.toLowerCase()}`;
            
            resultDiv.innerHTML = `
                <h4>${result.name}</h4>
                <p>
                    <span class="result-status ${result.status.toLowerCase()}">
                        ${result.status === 'SUCCESS' ? '✅ 実行開始' : '❌ エラー'}
                    </span>
                </p>
                ${result.execution_arn ? `
                    <p><strong>実行ARN:</strong> 
                        <a href="#" class="execution-link" onclick="gui.showExecutionDetails('${result.execution_arn}')">${result.execution_arn}</a>
                    </p>
                    <p><strong>開始時刻:</strong> ${result.start_date}</p>
                ` : ''}
                ${result.error ? `<p><strong>エラー:</strong> ${result.error}</p>` : ''}
                ${result.execution_arn ? `
                    <div class="log-copy" onclick="gui.copyToClipboard(this)" title="クリックでコピー">
                        ExecutionARN: ${result.execution_arn}
                    </div>
                ` : ''}
                ${result.error ? `
                    <div class="log-copy" onclick="gui.copyToClipboard(this)" title="クリックでコピー">
                        Error: ${result.error}
                    </div>
                ` : ''}
                <details style="margin-top: 0.5rem;">
                    <summary>入力データ</summary>
                    <pre><code>${JSON.stringify(result.input_data, null, 2)}</code></pre>
                </details>
            `;
            
            container.appendChild(resultDiv);
        });
        
        resultsSection.style.display = 'block';
        resultsSection.scrollIntoView({ behavior: 'smooth' });
    }

    async refreshStatus() {
        const successfulExecutions = this.executionResults.filter(r => r.status === 'SUCCESS');
        
        if (successfulExecutions.length === 0) {
            return;
        }

        const statusContainer = document.getElementById('status-container');
        statusContainer.innerHTML = '<p>🔄 状況を更新中...</p>';

        const statusPromises = successfulExecutions.map(execution => 
            this.getExecutionStatus(execution.execution_arn)
        );

        try {
            const statuses = await Promise.all(statusPromises);
            this.displayStatusUpdates(statuses);
        } catch (error) {
            statusContainer.innerHTML = `<p>❌ 状況更新エラー: ${error.message}</p>`;
        }
    }

    async getExecutionStatus(executionArn) {
        const response = await fetch(`/status/${encodeURIComponent(executionArn)}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        result.execution_arn = executionArn;
        return result;
    }

    displayStatusUpdates(statuses) {
        const statusContainer = document.getElementById('status-container');
        statusContainer.innerHTML = '';

        statuses.forEach(status => {
            const statusDiv = document.createElement('div');
            statusDiv.className = 'status-item';
            
            const executionName = status.execution_arn.split(':').pop();
            const duration = status.stop_date ? 
                this.calculateDuration(status.start_date, status.stop_date) : 
                this.calculateDuration(status.start_date, new Date().toISOString());

            statusDiv.innerHTML = `
                <div class="status-details">
                    <h5>${executionName}</h5>
                    <p><strong>実行時間:</strong> ${duration}</p>
                    ${status.error ? `<p><strong>エラー:</strong> ${status.error}</p>` : ''}
                    ${status.cause ? `<p><strong>原因:</strong> ${status.cause}</p>` : ''}
                </div>
                <div>
                    <span class="execution-status ${status.execution_status}">${status.execution_status}</span>
                </div>
            `;
            
            statusContainer.appendChild(statusDiv);
        });

        // 最終更新時刻を表示
        const updateTime = document.createElement('p');
        updateTime.style.textAlign = 'center';
        updateTime.style.color = '#718096';
        updateTime.style.marginTop = '1rem';
        updateTime.innerHTML = `最終更新: ${new Date().toLocaleString('ja-JP')}`;
        statusContainer.appendChild(updateTime);
    }

    calculateDuration(startDate, endDate) {
        const start = new Date(startDate);
        const end = new Date(endDate);
        const diffMs = end - start;
        const diffSeconds = Math.floor(diffMs / 1000);
        
        if (diffSeconds < 60) {
            return `${diffSeconds}秒`;
        } else if (diffSeconds < 3600) {
            const minutes = Math.floor(diffSeconds / 60);
            const seconds = diffSeconds % 60;
            return `${minutes}分${seconds}秒`;
        } else {
            const hours = Math.floor(diffSeconds / 3600);
            const minutes = Math.floor((diffSeconds % 3600) / 60);
            return `${hours}時間${minutes}分`;
        }
    }

    selectAll() {
        const checkboxes = document.querySelectorAll('input[name="selected_sfs"]');
        checkboxes.forEach(cb => cb.checked = true);
    }

    clearAll() {
        const checkboxes = document.querySelectorAll('input[name="selected_sfs"]');
        checkboxes.forEach(cb => cb.checked = false);
    }

    showExecutionDetails(executionArn) {
        // 実行詳細を表示（モーダルまたは新しいタブ）
        console.log('実行詳細:', executionArn);
        alert(`実行詳細:\n${executionArn}\n\n※AWS Consoleで詳細を確認してください`);
    }

    copyAllLogs() {
        if (this.executionResults.length === 0) {
            alert('コピーする実行結果がありません。');
            return;
        }
        
        // 実行ログ部分
        let allLogsText = `=== AWS ETL Evidence System 実行ログ ===\n`;
        allLogsText += `実行時刻: ${new Date().toISOString()}\n`;
        allLogsText += `実行パイプライン数: ${this.executionResults.length}\n\n`;
        
        this.executionResults.forEach((result, index) => {
            allLogsText += `${index + 1}. ${result.name}\n`;
            allLogsText += `   ステータス: ${result.status}\n`;
            
            if (result.status === 'SUCCESS') {
                allLogsText += `   実行ARN: ${result.execution_arn}\n`;
                allLogsText += `   開始時刻: ${result.start_date}\n`;
                allLogsText += `   バッチID: ${result.input_data.batch_id}\n`;
            } else {
                allLogsText += `   エラー: ${result.error}\n`;
            }
            allLogsText += `\n`;
        });

        // 監視ログ部分を追加
        const statusContainer = document.getElementById('status-container');
        if (statusContainer && statusContainer.children.length > 0) {
            allLogsText += `=== AWS ETL Evidence System 実行状況監視 ===\n`;
            allLogsText += `取得時刻: ${new Date().toISOString()}\n\n`;

            const statusItems = statusContainer.querySelectorAll('.status-item');
            statusItems.forEach((item, index) => {
                const nameElement = item.querySelector('h5');
                const detailElements = item.querySelectorAll('p');
                const statusElement = item.querySelector('.execution-status');

                if (nameElement) {
                    allLogsText += `${index + 1}. ${nameElement.textContent}\n`;
                }

                detailElements.forEach(detail => {
                    allLogsText += `   ${detail.textContent}\n`;
                });

                if (statusElement) {
                    allLogsText += `   ステータス: ${statusElement.textContent}\n`;
                }

                allLogsText += '\n';
            });

            // 最終更新時刻を取得
            const updateTimeElement = statusContainer.querySelector('p[style*="text-align: center"]');
            if (updateTimeElement) {
                allLogsText += `${updateTimeElement.textContent}\n`;
            }
        }
        
        navigator.clipboard.writeText(allLogsText).then(() => {
            const btn = document.getElementById('copy-all-logs');
            const originalText = btn.innerHTML;
            btn.innerHTML = '✅ コピー完了';
            setTimeout(() => {
                btn.innerHTML = originalText;
            }, 2000);
        }).catch(err => {
            console.error('クリップボードコピーエラー:', err);
            alert('ログのコピーに失敗しました。');
        });
    }

    copyToClipboard(element) {
        const text = element.textContent;
        navigator.clipboard.writeText(text).then(() => {
            const originalBg = element.style.backgroundColor;
            element.style.backgroundColor = '#c6f6d5';
            setTimeout(() => {
                element.style.backgroundColor = originalBg;
            }, 1000);
        }).catch(err => {
            console.error('クリップボードコピーエラー:', err);
        });
    }

}

// グローバルインスタンス作成
const gui = new ETLSystemGUI();