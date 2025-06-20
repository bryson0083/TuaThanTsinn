# -*- coding: utf-8 -*-
"""
DPL001 - 台股數據收集相關的 Prefect Flows
包含各種台股數據的資料抓取流程
"""

import os
from datetime import datetime
from pathlib import Path
import json
import sys
# from dotenv import load_dotenv
import papermill as pm


# 導入自建公用模組 - 載入時會自動初始化環境設定
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from TuaThanTsinn.proj_util_pkg.settings import settings

# 導入 Prefect 相關模組
from prefect import flow, task
from prefect.logging import get_run_logger

# 設定目錄路徑
CURRENT_DIR = Path(__file__).parent
PROJECT_ROOT = CURRENT_DIR.parent


@task(name="執行台股股票代碼資訊收集")
def run_dpl001_01_twstock_info_collect():
    """執行台股股票代碼資訊收集 - 使用 papermill 執行 collect_twstock_etf_list.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股股票代碼資訊收集...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_twstock_etf_list.ipynb"
        output_notebook = CURRENT_DIR / "collect_twstock_etf_list_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "DPL001_台股數據收集"
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📓 執行 notebook: {input_notebook}")
        
        # 使用 papermill 執行 notebook
        pm.execute_notebook(
            input_path=str(input_notebook),
            output_path=str(output_notebook),
            parameters={
                # 可以在這裡傳遞參數給 notebook
                # 例如: 'analysis_date': datetime.now().strftime('%Y-%m-%d')
            },
            log_output=True,
            progress_bar=False
        )
        
        logger.info("✅ Notebook 執行完成")

        # 儲存執行結果摘要
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_DPL001_01_台股股票代碼資訊收集_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "DPL001_01_台股股票代碼資訊收集",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ DPL001_01_台股股票代碼資訊收集 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_DPL001_01_台股股票代碼資訊收集_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行台股收盤資訊收集")
def run_dpl001_02_twstock_close_info_collect():
    """執行台股收盤資訊收集 - 使用 papermill 執行 collect_台股收盤資訊.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股收盤資訊收集...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_台股收盤資訊.ipynb"
        output_notebook = CURRENT_DIR / "collect_台股收盤資訊_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "DPL001_台股數據收集"
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📓 執行 notebook: {input_notebook}")
        
        # 使用 papermill 執行 notebook
        pm.execute_notebook(
            input_path=str(input_notebook),
            output_path=str(output_notebook),
            parameters={
                # 可以在這裡傳遞參數給 notebook
                # 例如: 'analysis_date': datetime.now().strftime('%Y-%m-%d')
            },
            log_output=True,
            progress_bar=False
        )
        
        logger.info("✅ Notebook 執行完成")

        # 儲存執行結果摘要
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_DPL001_02_台股收盤資訊收集_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "DPL001_02_台股收盤資訊收集",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ DPL001_02_台股收盤資訊收集 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_DPL001_02_台股收盤資訊收集_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行鉅亨網題材來源收集")
def run_dpl001_03_twstock_topic_collect():
    """執行鉅亨網題材來源收集 - 使用 papermill 執行 collect_鉅亨網題材來源.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行鉅亨網題材來源收集...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_鉅亨網題材來源.ipynb"
        output_notebook = CURRENT_DIR / "collect_鉅亨網題材來源_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "DPL001_台股數據收集"
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📓 執行 notebook: {input_notebook}")
        
        # 使用 papermill 執行 notebook
        pm.execute_notebook(
            input_path=str(input_notebook),
            output_path=str(output_notebook),
            parameters={
                # 可以在這裡傳遞參數給 notebook
                # 例如: 'analysis_date': datetime.now().strftime('%Y-%m-%d')
            },
            log_output=True,
            progress_bar=False
        )
        
        logger.info("✅ Notebook 執行完成")

        # 儲存執行結果摘要
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_DPL001_03_鉅亨網題材來源收集_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "DPL001_03_鉅亨網題材來源收集",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ DPL001_03_鉅亨網題材來源收集 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_DPL001_03_鉅亨網題材來源收集_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@flow(name="DPL001_台股數據收集", log_prints=True)
def do_dpl001_flow():
    """ DPL001_台股數據收集流程 """
    
    logger = get_run_logger()
    logger.info("🚀 開始執行 DPL001_台股數據收集 流程")
    
    try:
        # 並行執行各種選股分析
        dpl001_01_twstock_info_result = run_dpl001_01_twstock_info_collect()
        dpl001_02_twstock_close_info_result = run_dpl001_02_twstock_close_info_collect()

        # 收集所有結果
        all_results = [dpl001_01_twstock_info_result, dpl001_02_twstock_close_info_result]

        logger.info("🎉 DPL001_台股數據收集 流程完成！")
        return {
            "status": "success", 
            "analysis_results": all_results,
            "analysis_complete": True
        }
        
    except Exception as e:
        logger.error(f"❌ DPL001_台股數據收集 流程發生錯誤: {str(e)}")
        return {
            "status": "failed", 
            "error": str(e),
            "analysis_complete": False
        }


@flow(name="DPL001_台股數據收集_鉅亨網題材來源", log_prints=True)
def do_dpl001_flow_topic():
    """ DPL001_台股數據收集_鉅亨網題材來源流程 """
    
    logger = get_run_logger()
    logger.info("🚀 開始執行 DPL001_台股數據收集_鉅亨網題材來源 流程")
    
    try:
        # 並行執行各種選股分析
        dpl001_03_twstock_topic_result = run_dpl001_03_twstock_topic_collect()

        # 收集所有結果
        all_results = [dpl001_03_twstock_topic_result]

        logger.info("🎉 DPL001_台股數據收集_鉅亨網題材來源 流程完成！")
        return {
            "status": "success", 
            "analysis_results": all_results,
            "analysis_complete": True
        }
        
    except Exception as e:
        logger.error(f"❌ DPL001_台股數據收集_鉅亨網題材來源 流程發生錯誤: {str(e)}")
        return {
            "status": "failed", 
            "error": str(e),
            "analysis_complete": False
        }


if __name__ == "__main__":
    # 直接執行此檔案時，執行 DPL001_台股數據收集 流程
    result = do_dpl001_flow()
    print(f"Flow execution result: {result}")
    