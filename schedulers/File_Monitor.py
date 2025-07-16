import os
import sys
import time
import subprocess
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# File paths - Work and Home configurations
INPUT_DIR = "/home/darvin/Fox_ETL/input"

WORKSTATION_XLS_FILENAME = "workstationOutputReport.xls"
TESTBOARD_XLS_FILENAME = "Test board record report.xls"
SNFN_XLS_FILENAME = "snfnReport.xls"
WORKSTATION_FILEPATH = os.path.join(INPUT_DIR, WORKSTATION_XLS_FILENAME)
TESTBOARD_FILEPATH = os.path.join(INPUT_DIR, TESTBOARD_XLS_FILENAME)
SNFN_FILEPATH = os.path.join(INPUT_DIR, SNFN_XLS_FILENAME)

# Import script paths (relative to ETL_V2 directory)
ETL_V2_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMPORT_TESTBOARD_SCRIPT = os.path.join(ETL_V2_DIR, "loaders", "import_testboard_file.py")
IMPORT_WORKSTATION_SCRIPT = os.path.join(ETL_V2_DIR, "loaders", "import_workstation_file.py")
IMPORT_SNFN_SCRIPT = os.path.join(ETL_V2_DIR, "loaders", "import_snfn_file.py")

def convert_xls_to_xlsx(xls_file_path):
    """
    Convert XLS file to XLSX using LibreOffice
    Returns the path to the converted XLSX file, or None if conversion failed
    """
    try:
        # Generate XLSX file path
        xlsx_file_path = os.path.splitext(xls_file_path)[0] + '.xlsx'
        
        logger.info(f"Converting {os.path.basename(xls_file_path)} to XLSX...")
        
        # Use LibreOffice to convert the file
        cmd = [
            'libreoffice',
            '--headless',
            '--convert-to', 'xlsx',
            '--outdir', os.path.dirname(xls_file_path),
            xls_file_path
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        # Run the conversion
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60  # 60 second timeout
        )
        
        if result.returncode == 0:
            logger.info(f"✅ Successfully converted to {os.path.basename(xlsx_file_path)}")
            return xlsx_file_path
        else:
            logger.error(f"❌ LibreOffice conversion failed: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ LibreOffice conversion timed out for {os.path.basename(xls_file_path)}")
        return None
    except Exception as e:
        logger.error(f"❌ Error converting {os.path.basename(xls_file_path)}: {e}")
        return None

def process_file(file_path, script_path, file_type):
    """
    Process a file by converting it to XLSX and then importing it
    """
    try:
        # Convert XLS to XLSX
        xlsx_file_path = convert_xls_to_xlsx(file_path)
        
        if not xlsx_file_path:
            logger.error(f"❌ Failed to convert {os.path.basename(file_path)} to XLSX")
            return False
        
        # Check if XLSX file was created
        if not os.path.exists(xlsx_file_path):
            logger.error(f"❌ XLSX file not found after conversion: {os.path.basename(xlsx_file_path)}")
            return False
        
        # Delete original XLS file immediately after successful conversion
        try:
            os.remove(file_path)
            logger.info(f"🗑️ Deleted original XLS file: {os.path.basename(file_path)}")
        except Exception as e:
            logger.warning(f"⚠️ Could not delete original XLS file: {e}")
        
        logger.info(f"📥 Importing {file_type} data using {os.path.basename(script_path)}...")
        
        # Run the import script
        cmd = ['python3', script_path, xlsx_file_path]
        logger.info(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout for import
        )
        
        if result.returncode == 0:
            logger.info(f"✅ Successfully imported {file_type} data")
            logger.info(f"Output: {result.stdout}")
            return True
        else:
            logger.error(f"❌ Import script failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Import script timed out for {file_type}")
        return False
    except Exception as e:
        logger.error(f"❌ Error processing {file_type}: {e}")
        return False

def monitor_for_files():
    """
    Infinite loop that watches for XLS files and triggers processing
    """
    logger.info("🚀 Starting file monitor for PostgreSQL ETL pipeline")
    logger.info(f"📁 Monitoring directory: {INPUT_DIR}")
    logger.info(f"🎯 Target files: {WORKSTATION_XLS_FILENAME}, {TESTBOARD_XLS_FILENAME}")
    logger.info(f"📊 Import scripts: {os.path.basename(IMPORT_WORKSTATION_SCRIPT)}, {os.path.basename(IMPORT_TESTBOARD_SCRIPT)}")
    
    while True:
        try:
            # Check for workstation output report
            if os.path.exists(WORKSTATION_FILEPATH):
                logger.info(f"📋 STEP 1: Workstation file detected: {WORKSTATION_XLS_FILENAME} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"🔄 STEP 2: Starting workstation file processing pipeline...")
                
                success = process_file(
                    WORKSTATION_FILEPATH, 
                    IMPORT_WORKSTATION_SCRIPT, 
                    "workstation"
                )
                
                if success:
                    logger.info(f"✅ STEP 3: Workstation file processing completed successfully")
                else:
                    logger.error(f"❌ STEP 3: Workstation file processing failed")
            
            # Check for test board report
            if os.path.exists(TESTBOARD_FILEPATH):
                logger.info(f"📋 STEP 1: Test board file detected: {TESTBOARD_XLS_FILENAME} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"🔄 STEP 2: Starting test board file processing pipeline...")
                
                success = process_file(
                    TESTBOARD_FILEPATH, 
                    IMPORT_TESTBOARD_SCRIPT, 
                    "testboard"
                )
                
                if success:
                    logger.info(f"✅ STEP 3: Test board file processing completed successfully")
                else:
                    logger.error(f"❌ STEP 3: Test board file processing failed")

            # Check for snfn report
            if os.path.exists(SNFN_FILEPATH):
                logger.info(f"📋 STEP 1: SnfN file detected: {SNFN_XLS_FILENAME} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"🔄 STEP 2: Starting SnFn file processing pipeline...")
                
                success = process_file(
                    SnFn_FILEPATH, 
                    IMPORT_SnFn_SCRIPT, 
                    "snfn"
                )
                
                if success:
                    logger.info(f"✅ STEP 3: snfn file processing completed successfully")
                else:
                    logger.error(f"❌ STEP 3: snfn file processing failed")
                
            time.sleep(10)  # Check every 10 seconds
            
        except KeyboardInterrupt:
            logger.info("🛑 File monitor shutdown requested")
            break
        except Exception as e:
            logger.error(f"❌ STEP ERROR: Error in monitor loop: {e}")
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(10)

if __name__ == "__main__":
    monitor_for_files() 