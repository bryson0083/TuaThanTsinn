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


@task(name="執行麥克連大盤脈動_大盤指數收盤資訊")
def run_ana003_01_barometer_collect():
    """執行麥克連大盤脈動_大盤指數收盤資訊 - 使用 papermill 執行 collect_麥克連大盤脈動_大盤指數收盤資訊.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行麥克連大盤脈動_大盤指數收盤資訊...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_1_大盤指數收盤資訊.ipynb"
        output_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_1_大盤指數收盤資訊_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA003_麥克連大盤脈動"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_01_麥克連大盤脈動_part_1_大盤指數收盤資訊_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA003_01_麥克連大盤脈動_part_1_大盤指數收盤資訊",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA003_01_麥克連大盤脈動_part_1_大盤指數收盤資訊 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_01_麥克連大盤脈動_part_1_大盤指數收盤資訊_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行麥克連大盤脈動_融資融券餘額")
def run_ana003_02_barometer_collect():
    """執行麥克連大盤脈動_融資融券餘額 - 使用 papermill 執行 collect_麥克連大盤脈動_融資融券餘額.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行麥克連大盤脈動_融資融券餘額...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_2_融資融券餘額.ipynb"
        output_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_2_融資融券餘額_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA003_麥克連大盤脈動"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_02_麥克連大盤脈動_part_2_融資融券餘額_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA003_02_麥克連大盤脈動_part_2_融資融券餘額",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA003_02_麥克連大盤脈動_part_2_融資融券餘額 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_02_麥克連大盤脈動_part_2_融資融券餘額_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行麥克連大盤脈動_美股大盤收盤資訊")
def run_ana003_03_barometer_collect():
    """執行麥克連大盤脈動_美股大盤收盤資訊 - 使用 papermill 執行 collect_麥克連大盤脈動_美股大盤收盤資訊.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行麥克連大盤脈動_美股大盤收盤資訊...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_3_美股大盤收盤資訊.ipynb"
        output_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_3_美股大盤收盤資訊_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA003_麥克連大盤脈動"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_03_麥克連大盤脈動_part_3_美股大盤收盤資訊_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA003_03_麥克連大盤脈動_part_3_美股大盤收盤資訊",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA003_03_麥克連大盤脈動_part_3_美股大盤收盤資訊 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_03_麥克連大盤脈動_part_3_美股大盤收盤資訊_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行麥克連大盤脈動_三大法人買賣超")
def run_ana003_04_barometer_collect():
    """執行麥克連大盤脈動_三大法人買賣超 - 使用 papermill 執行 collect_麥克連大盤脈動_三大法人買賣超.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行麥克連大盤脈動_三大法人買賣超...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_4_三大法人買賣超.ipynb"
        output_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_4_三大法人買賣超_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA003_麥克連大盤脈動"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_04_麥克連大盤脈動_part_4_三大法人買賣超_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA003_04_麥克連大盤脈動_part_4_三大法人買賣超",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA003_04_麥克連大盤脈動_part_4_三大法人買賣超 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_04_麥克連大盤脈動_part_4_三大法人買賣超_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行麥克連大盤脈動_美元兌台幣匯率")
def run_ana003_05_barometer_collect():
    """執行麥克連大盤脈動_美元兌台幣匯率 - 使用 papermill 執行 collect_麥克連大盤脈動_美元兌台幣匯率.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行麥克連大盤脈動_美元兌台幣匯率...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_5_美元兌台幣匯率.ipynb"
        output_notebook = CURRENT_DIR / "collect_麥克連大盤脈動_part_5_美元兌台幣匯率_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA003_麥克連大盤脈動"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_05_麥克連大盤脈動_part_5_美元兌台幣匯率_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "ANA003_05_麥克連大盤脈動_part_5_美元兌台幣匯率",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ ANA003_05_麥克連大盤脈動_part_5_美元兌台幣匯率 發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA003_05_麥克連大盤脈動_part_5_美元兌台幣匯率_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@flow(name="ANA003_麥克連大盤脈動", log_prints=True)
def do_ana003_flow():
    """ ANA003_麥克連大盤脈動流程 """
    
    logger = get_run_logger()
    logger.info("🚀 開始執行 ANA003_麥克連大盤脈動 流程")
    
    try:
        # 並行執行各種選股分析
        ana003_01_barometer_result = run_ana003_01_barometer_collect()
        ana003_02_barometer_result = run_ana003_02_barometer_collect()
        ana003_03_barometer_result = run_ana003_03_barometer_collect()
        ana003_04_barometer_result = run_ana003_04_barometer_collect()
        ana003_05_barometer_result = run_ana003_05_barometer_collect()

        # 收集所有結果
        all_results = [ana003_01_barometer_result, ana003_02_barometer_result, ana003_03_barometer_result, ana003_04_barometer_result, ana003_05_barometer_result]

        logger.info("🎉 ANA003_麥克連大盤脈動 流程完成！")
        return {
            "status": "success", 
            "analysis_results": all_results,
            "analysis_complete": True
        }
        
    except Exception as e:
        logger.error(f"❌ ANA003_麥克連大盤脈動 流程發生錯誤: {str(e)}")
        return {
            "status": "failed", 
            "error": str(e),
            "analysis_complete": False
        }


if __name__ == "__main__":
    # 直接執行此檔案時，執行 ANA003_麥克連大盤脈動 流程
    result = do_ana003_flow()
    print(f"Flow execution result: {result}")
    