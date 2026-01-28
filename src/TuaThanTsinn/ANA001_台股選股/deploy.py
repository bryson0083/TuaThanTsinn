# -*- coding: utf-8 -*-
"""
ANA001 - 台股選股相關的 Prefect Flows
包含各種技術指標和籌碼面的選股流程
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


@task(name="執行KD選股分析")
def run_kd_stock_selection():
    """執行KD指標選股分析 - 使用 papermill 執行 twstock_kd選股.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行KD選股分析...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "twstock_kd選股.ipynb"
        output_notebook = CURRENT_DIR / "twstock_kd選股_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA001_選股結果"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_01_kd_analysis_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "KD指標",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ KD選股分析發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_01_kd_analysis_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行DMI選股分析")
def run_dmi_stock_selection():
    """執行DMI指標選股分析 - 使用 papermill 執行 twstock_dmi選股.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行DMI選股分析...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "twstock_dmi選股.ipynb"
        output_notebook = CURRENT_DIR / "twstock_dmi選股_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA001_選股結果"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_02_dmi_analysis_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "DMI指標",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ DMI選股分析發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_02_dmi_analysis_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行籌碼選股分析")
def run_chips_stock_selection():
    """執行籌碼選股分析 - 使用 papermill 執行 twstock_籌碼選股.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行籌碼選股分析...")
    
    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "twstock_籌碼選股.ipynb"
        output_notebook = CURRENT_DIR / "twstock_籌碼選股_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA001_選股結果"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_03_chips_analysis_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "籌碼選股",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ 籌碼選股分析發生錯誤: {str(e)}")
        
        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }
        
        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_03_chips_analysis_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)
            
        raise


@task(name="執行布林通道選股分析")
def run_bollinger_stock_selection():
    """執行布林通道選股分析 - 使用 papermill 執行 twstock_布林通道選股.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行布林通道選股分析...")

    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "twstock_布林通道選股.ipynb"
        output_notebook = CURRENT_DIR / "twstock_布林通道選股_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA001_選股結果"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_04_bollinger_analysis_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "布林通道選股",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ 布林通道選股分析發生錯誤: {str(e)}")

        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }

        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_04_bollinger_analysis_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)

        raise


@task(name="執行均線多排選股分析")
def run_ma_alignment_stock_selection():
    """執行均線多排選股分析 - 使用 papermill 執行 twstock_均線多排選股.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行均線多排選股分析...")

    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "twstock_均線多排選股.ipynb"
        output_notebook = CURRENT_DIR / "twstock_均線多排選股_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA001_選股結果"
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
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_05_ma_alignment_analysis_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "均線多排選股",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ 均線多排選股分析發生錯誤: {str(e)}")

        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }

        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_05_ma_alignment_analysis_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)

        raise


@task(name="執行CIS選股分析")
def run_cis_stock_selection():
    """執行CIS選股分析 - 使用 papermill 執行 twstock_cis選股.ipynb"""

    logger = get_run_logger()
    logger.info("🔍 開始執行CIS選股分析...")

    try:
        # 定義檔案路徑
        input_notebook = CURRENT_DIR / "twstock_cis選股.ipynb"
        output_notebook = CURRENT_DIR / "twstock_cis選股_executed.ipynb"

        # 確保輸出目錄存在
        output_dir = PROJECT_ROOT / "output" / "ANA001_選股結果"
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📓 執行 notebook: {input_notebook}")

        # 使用 papermill 執行 notebook
        pm.execute_notebook(
            input_path=str(input_notebook),
            output_path=str(output_notebook),
            parameters={},
            log_output=True,
            progress_bar=False
        )

        logger.info("✅ Notebook 執行完成")

        # 儲存執行結果摘要
        summary_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_06_cis_analysis_deploy.json"
        summary = {
            "analysis_time": datetime.now().isoformat(),
            "notebook_executed": str(output_notebook),
            "success": True
        }

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info(f"📊 執行摘要已儲存至: {summary_path}")

        return {
            "method": "CIS選股",
            "output_file": str(output_notebook),
            "analysis_time": datetime.now().isoformat(),
            "notebook_path": str(output_notebook)
        }

    except Exception as e:
        logger.error(f"❌ CIS選股分析發生錯誤: {str(e)}")

        # 儲存錯誤資訊
        error_summary = {
            "analysis_time": datetime.now().isoformat(),
            "error_message": str(e),
            "success": False
        }

        error_path = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M')}_ANA001_06_cis_analysis_deploy_error.json"
        with open(error_path, 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)

        raise


@flow(name="ANA001_台股選股", log_prints=True)
def do_ana001_flow():
    """ ANA001_台股選股流程 """
    logger = get_run_logger()
    logger.info("🚀 開始執行 ANA001_台股選股 流程")

    try:
        # 並行執行各種選股分析
        ana001_01_kd_result = run_kd_stock_selection()
        ana001_02_dmi_result = run_dmi_stock_selection()
        ana001_03_chips_result = run_chips_stock_selection()
        ana001_04_bollinger_result = run_bollinger_stock_selection()
        ana001_05_ma_alignment_result = run_ma_alignment_stock_selection()
        ana001_06_cis_result = run_cis_stock_selection()

        # 收集所有結果
        all_results = [
            ana001_01_kd_result,
            ana001_02_dmi_result,
            ana001_03_chips_result,
            ana001_04_bollinger_result,
            ana001_05_ma_alignment_result,
            ana001_06_cis_result,
        ]

        logger.info("🎉 ANA001_台股選股 流程完成！")
        return {
            "status": "success",
            "analysis_results": all_results,
            "analysis_complete": True
        }

    except Exception as e:
        logger.error(f"❌ ANA001_台股選股 流程發生錯誤: {str(e)}")
        return {
            "status": "failed",
            "error": str(e),
            "analysis_complete": False
        }


if __name__ == "__main__":
    # 直接執行此檔案時，執行 ANA001_台股選股 流程
    result = do_ana001_flow()
    print(f"Flow execution result: {result}")
    