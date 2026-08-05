import os
import sys
import time
import shutil
import logging

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# Import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import PATHS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def setup_driver():
    try:
        downloads_path = PATHS['downloads_dir']

        chrome_options = Options()

        prefs = {
            "download.default_directory": downloads_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }

        chrome_options.add_experimental_option("prefs", prefs)

        # Disable Chrome download protection
        chrome_options.add_argument("--safebrowsing-disable-download-protection")

        # Optional improvements
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")

        # Persistent Chrome profile
        # chrome_profile = os.path.join(downloads_path, "chrome_profile")
        chrome_profile = os.path.expanduser(
            "~/.local/share/file_extractor/chrome_profile"
        )
        os.makedirs(chrome_profile, exist_ok=True)

        chrome_options.add_argument(
            f"--user-data-dir={chrome_profile}"
        )

        driver = webdriver.Chrome(options=chrome_options)

        # Force allow downloads
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": downloads_path
            },
        )

        logger.info("Chrome driver initialized successfully")

        return driver

    except Exception as e:
        logger.error(f"Error setting up Chrome driver: {e}")
        return None


def delete_old_file(filename):
    try:
        downloads_path = PATHS['downloads_dir']

        file_path = os.path.join(downloads_path, filename)

        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted old file: {filename}")

        partial_file = file_path + ".crdownload"

        if os.path.exists(partial_file):
            os.remove(partial_file)
            logger.info(f"Deleted partial download: {partial_file}")

    except Exception as e:
        logger.warning(f"Could not delete old file {filename}: {e}")


def monitor_and_move_file(filename):
    downloads_path = PATHS['downloads_dir']
    target_path = PATHS['input_dir']

    source_file = os.path.join(downloads_path, filename)
    target_file = os.path.join(target_path, filename)

    try:
        if os.path.exists(source_file):

            # Remove existing target file if needed
            if os.path.exists(target_file):
                os.remove(target_file)

            shutil.move(source_file, target_file)

            logger.info(f"File moved to processing queue: {filename}")

            return True

        return False

    except Exception as e:
        logger.error(
            f"Error moving file from {source_file} to {target_file}: {e}"
        )
        return False


def wait_for_download(filename, timeout=120):
    downloads_path = PATHS['downloads_dir']

    file_path = os.path.join(downloads_path, filename)
    partial_file = file_path + ".crdownload"

    start_time = time.time()

    while time.time() - start_time < timeout:

        # Download completed
        if os.path.exists(file_path) and not os.path.exists(partial_file):
            time.sleep(2)
            return True

        time.sleep(1)

    return False


def click_confirm_button(driver):
    confirm_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//span[normalize-space(text())='confirm']")
        )
    )

    time.sleep(2)

    confirm_button.click()


def process_download(driver, filename, report_name):
    try:
        delete_old_file(filename)

        click_confirm_button(driver)

        logger.info(f"Clicked confirm for {report_name}")

        if wait_for_download(filename):

            logger.info(f"{report_name} download completed")

            moved = monitor_and_move_file(filename)

            if moved:
                logger.info(f"{report_name} moved successfully")
            else:
                logger.error(f"Failed moving {report_name}")

        else:
            logger.error(f"{report_name} download timed out")

    except Exception as e:
        logger.error(f"Error processing {report_name}: {e}")


def main():

    driver = setup_driver()

    if not driver:
        return

    try:

        driver.get("https://wareconn.com/r/Summary/pctls")

        input(
            "\n"
            "Login and configure BOTH forms in two tabs.\n\n"
            "Tab 1 = workstationOutputReport.xls\n"
            "Tab 2 = Test board record report.xls\n\n"
            "Press ENTER when ready...\n"
        )

        # Open second tab
        driver.execute_script(
            "window.open('https://wareconn.com/r/Summary/pctls', '_blank');"
        )

        time.sleep(3)

        tabs = driver.window_handles

        tab1 = tabs[0]
        tab2 = tabs[1]

        driver.switch_to.window(tab2)

        input(
            "\n"
            "Configure second tab for:\n"
            "Test board record report.xls\n\n"
            "Press ENTER when ready...\n"
        )

        workstation_filename = "workstationOutputReport.xls"
        testboard_filename = "Test board record report.xls"

        logger.info("Starting automated download loop...")

        while True:

            # ==========================
            # TAB 1 - WORKSTATION
            # ==========================

            driver.switch_to.window(tab1)

            process_download(
                driver,
                workstation_filename,
                "Workstation report"
            )

            # ==========================
            # TAB 2 - TESTBOARD
            # ==========================

            driver.switch_to.window(tab2)

            process_download(
                driver,
                testboard_filename,
                "Testboard report"
            )

            # ==========================
            # WAIT BEFORE NEXT CYCLE
            # ==========================

            logger.info("Waiting 120 seconds before next cycle...")

            time.sleep(120)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")

    except Exception as e:
        logger.error(f"Fatal error during extraction process: {e}")

    finally:
        driver.quit()
        logger.info("Chrome driver closed")


if __name__ == "__main__":
    main()