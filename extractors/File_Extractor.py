from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import shutil
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver():
    try:
        driver = webdriver.Chrome()
        return driver
    except Exception as e:
        logger.error(f"Error setting up Chrome driver: {e}")
        return None

def monitor_and_move_file(filename):
    # Linux paths
    downloads_path = "/home/darvin/Downloads"
    target_path = "/home/darvin/Fox-ETL/input"

    source_file = os.path.join(downloads_path, filename)
    target_file = os.path.join(target_path, filename)

    try:
        # Only proceed if source file exists
        if os.path.exists(source_file):
            shutil.move(source_file, target_file)
            logger.info(f"File moved to processing queue: {filename}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error moving file from {source_file} to {target_file}: {e}")
        return False

def main():
    driver = setup_driver()
    if driver:
        try:
            # Open first tab and navigate to the first report page
            driver.get("https://wareconn.com/r/Summary/pctls")
            input("Login and configure BOTH forms in two tabs, then press Enter to start auto-submit...\n\nTab 1: workstationOutputReport.xls\nTab 2: Test board record report.xls\n\nMake sure both tabs are ready and on the correct form page.")

            # Open second tab for the second report
            driver.execute_script("window.open('https://wareconn.com/r/Summary/pctls', '_blank');")
            time.sleep(2)
            tabs = driver.window_handles
            tab1 = tabs[0]
            tab2 = tabs[1]

            # Let user configure the second tab
            driver.switch_to.window(tab2)
            input("Configure the second tab for 'Test board record report.xls', then press Enter to continue...")
            driver.switch_to.window(tab1)

            workstation_filename = "workstationOutputReport.xls"
            testboard_filename = "Test board record report.xls"

            while True:
                # --- Tab 1: workstationOutputReport.xls ---
                driver.switch_to.window(tab1)
                try:
                    confirm_button1 = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[normalize-space(text())='confirm']"))
                    )
                    confirm_button1.click()
                    logger.info("Clicked confirm on Tab 1 (workstation report)")
                except Exception as e:
                    logger.error(f"Could not click confirm button on Tab 1: {e}")
                time.sleep(5)
                monitor_and_move_file(workstation_filename)

                # --- Tab 2: Test board record report.xls ---
                driver.switch_to.window(tab2)
                try:
                    confirm_button2 = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[normalize-space(text())='confirm']"))
                    )
                    confirm_button2.click()
                    logger.info("Clicked confirm on Tab 2 (test board report)")
                except Exception as e:
                    logger.error(f"Could not click confirm button on Tab 2: {e}")
                time.sleep(5)
                monitor_and_move_file(testboard_filename)

                # Switch back to Tab 1 and wait before next cycle
                driver.switch_to.window(tab1)
                time.sleep(120)  # Wait 2 minutes before next cycle

        except Exception as e:
            logger.error(f"Error during extraction process: {e}")
        finally:
            driver.quit()

if __name__ == "__main__":
    main() 