# TuaThanTsinn
## Step 1: 啟用 Prefect 服務.  
- 開啟終端機，進入專案根目錄，執行以下指令：
    ```bash
    # 啟動虛擬環境
    source venv/bin/activate

    # 進入 Ｐrefect 服務目錄
    cd src/TuaThanTsinn/prefect_service

    # 啟動 Prefect 服務
    python start_server.py

    # 註冊 Flow
    python simple_deploy.py
    ```

## Step 2: 部署 Prefect 排程.  
 - 開啟終端機，進入專案根目錄，執行以下指令：
    ```bash
    # 啟動虛擬環境
    source venv/bin/activate

    # 進入 Ｐrefect 服務目錄
    cd src/TuaThanTsinn/prefect_service

    # 部署排程
    python simple_deploy.py
    ```