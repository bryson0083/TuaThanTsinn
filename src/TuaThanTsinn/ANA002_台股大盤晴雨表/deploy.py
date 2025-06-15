# -*- coding: utf-8 -*-
"""
ANA002 - 台股大盤晴雨表相關的 Prefect Flows
包含各種晴雨表的資料抓取流程
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


@task(name="執行台股晴雨表_景氣對策燈號")
def run_ana002_01_barometer_collect():
    """執行台股晴雨表_景氣對策燈號 - 使用 papermill 執行 collect_台股晴雨表_景氣對策燈號.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股晴雨表_景氣對策燈號...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_台股晴雨表_景氣對策燈號.ipynb"
        output_notebook = CURRENT_DIR / "collect_台股晴雨表_景氣對策燈號_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA002_台股大盤晴雨表"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_01_台股晴雨表_景氣對策燈號_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA002_01_台股晴雨表_景氣對策燈號",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA002_01_台股晴雨表_景氣對策燈號 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_01_台股晴雨表_景氣對策燈號_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行台股晴雨表_台指期貨三大法人期貨淨未平倉量口數")
def run_ana002_02_barometer_collect():
    """執行台股晴雨表_台指期貨三大法人期貨淨未平倉量口數 - 使用 papermill 執行 collect_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股晴雨表_台指期貨三大法人期貨淨未平倉量口數...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數.ipynb"
        output_notebook = CURRENT_DIR / "collect_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA002_台股大盤晴雨表"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_02_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA002_02_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA002_02_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_02_台股晴雨表_台指期貨三大法人期貨淨未平倉量口數_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行台股晴雨表_自營商選擇權")
def run_ana002_03_barometer_collect():
    """執行台股晴雨表_自營商選擇權 - 使用 papermill 執行 collect_台股晴雨表_自營商選擇權.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股晴雨表_自營商選擇權...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_台股晴雨表_自營商選擇權.ipynb"
        output_notebook = CURRENT_DIR / "collect_台股晴雨表_自營商選擇權_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA002_台股大盤晴雨表"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_03_台股晴雨表_自營商選擇權_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA002_03_台股晴雨表_自營商選擇權",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA002_03_台股晴雨表_自營商選擇權 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_03_台股晴雨表_自營商選擇權_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行台股晴雨表_散戶小台微台淨未平倉量")
def run_ana002_04_barometer_collect():
    """執行台股晴雨表_散戶小台微台淨未平倉量 - 使用 papermill 執行 collect_台股晴雨表_散戶小台微台淨未平倉量.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股晴雨表_散戶小台微台淨未平倉量...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_台股晴雨表_散戶小台微台淨未平倉量.ipynb"
        output_notebook = CURRENT_DIR / "collect_台股晴雨表_散戶小台微台淨未平倉量_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA002_台股大盤晴雨表"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_04_台股晴雨表_散戶小台微台淨未平倉量_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA002_04_台股晴雨表_散戶小台微台淨未平倉量",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA002_04_台股晴雨表_散戶小台微台淨未平倉量 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_04_台股晴雨表_散戶小台微台淨未平倉量_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行台股晴雨表_台指選擇權put_call_ratio值抓取")
def run_ana002_05_barometer_collect():
    """執行台股晴雨表_台指選擇權put_call_ratio值抓取 - 使用 papermill 執行 collect_台股晴雨表_台指選擇權put_call_ratio值抓取.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行台股晴雨表_台指選擇權put_call_ratio值抓取...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_台股晴雨表_台指選擇權put_call_ratio值抓取.ipynb"
        output_notebook = CURRENT_DIR / "collect_台股晴雨表_台指選擇權put_call_ratio值抓取_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA002_台股大盤晴雨表"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_05_台股晴雨表_台指選擇權put_call_ratio值抓取_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA002_05_台股晴雨表_台指選擇權put_call_ratio值抓取",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA002_05_台股晴雨表_台指選擇權put_call_ratio值抓取 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA002_05_台股晴雨表_台指選擇權put_call_ratio值抓取_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@flow(name="ANA002_台股大盤晴雨表", log_prints=True)
def do_ana002_flow():
    """ ANA002_台股大盤晴雨表流程 """
    
    logger = get_run_logger()
    logger.info("🚀 開始執行 ANA002_台股大盤晴雨表 流程")
    
    try:
        # 並行執行各種選股分析
        ana002_01_barometer_result = run_ana002_01_barometer_collect()
        ana002_02_barometer_result = run_ana002_02_barometer_collect()
        ana002_03_barometer_result = run_ana002_03_barometer_collect()
        ana002_04_barometer_result = run_ana002_04_barometer_collect()
        ana002_05_barometer_result = run_ana002_05_barometer_collect()

        # 收集所有結果
        all_results = [ana002_01_barometer_result, ana002_02_barometer_result, ana002_03_barometer_result, ana002_04_barometer_result, ana002_05_barometer_result]

        logger.info("🎉 ANA002_台股大盤晴雨表 流程完成！")
        return {
            "status": "success", 
            "analysis_results": all_results,
            "analysis_complete": True
        }
        
    except Exception as e:
        logger.error(f"❌ ANA002_台股大盤晴雨表 流程發生錯誤: {str(e)}")
        return {
            "status": "failed", 
            "error": str(e),
            "analysis_complete": False
        }


if __name__ == "__main__":
    # 直接執行此檔案時，執行 ANA002_台股大盤晴雨表 流程
    result = do_ana002_flow()
    print(f"Flow execution result: {result}")
    