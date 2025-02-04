import os
import shutil
import glob
import logging
import time
import error_handling
import pandas as pd

logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("===========================================")
logging.info('Process 10: Move Processed Files to Storage')
logging.info("===========================================")

print("\n============================================")
print(f"Process 10: Move Processed Files to Storage")
print("============================================")

try: 
    ori_path = r"\\atdfile3.ch.intel.com\catts\export\csv\vtiang\Vmin_dummy\dummy_site\dummy_group\HIST"
    with open('data_folder_path.txt', 'r') as file:
        data_folder_path = file.read().strip()

    # Rename rawdata files
    rawdata_files = glob.glob(os.path.join(ori_path,"rawdata_*"))
    for rawdata_file in rawdata_files:

        df = pd.read_csv(rawdata_file, nrows=5)
        lot_number = df['fac_lot'].iloc[0]
        new_rawdata_file_name = f"rawdata_{lot_number}.csv"
        new_rawdata_file_path = os.path.join(ori_path, new_rawdata_file_name)
        os.rename(rawdata_file, new_rawdata_file_path)

    # Get all files in the ori directory
    all_files = glob.glob(os.path.join(ori_path, "*"))

    # Function to move files with retry logic
    def move_file_with_retry(src, dst, retries=3, delay=5):
        for attempt in range(retries):
            try:
                shutil.copy(src, dst)  # Copy the file
                os.remove(src)  # Remove the original file
                print(f"Moved {os.path.basename(src)} to {dst}")
                logging.info(f"Moved {os.path.basename(src)} to {dst}")
                return True
            except Exception as e:
                print(f"Failed to move {os.path.basename(src)}: {e}")
                logging.error(f"Failed to move {os.path.basename(src)}: {e}")
                if attempt < retries - 1:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"Giving up on moving {os.path.basename(src)} after {retries} attempts.")
                    logging.error(f"Giving up on moving {os.path.basename(src)} after {retries} attempts.")
                    return False
    print(f"Moving rawdata files...")
    # Move each file to the destination directory
    for file_path in all_files:
        filename = os.path.basename(file_path)
        new_path = os.path.join(data_folder_path, filename)
        move_file_with_retry(file_path, new_path)

    # Clear copied vmin folder in local env
    folder_path = os.path.join(os.path.dirname(__file__), "VminFilesPlot")
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        try:
            # Delete the folder and all its contents
            shutil.rmtree(folder_path)
            print(f"The folder '{folder_path}' has been deleted successfully.")
            logging.info(f"The folder '{folder_path}' has been deleted successfully.")
        except OSError as e:
            print(f"Error: {folder_path} : {e.strerror}")
            logging.error(f"Error: {folder_path} : {e.strerror}")
    else:
        print(f"The folder '{folder_path}' does not exist or is not a directory.")
        logging.info(f"The folder '{folder_path}' does not exist or is not a directory.")

    #Move boxplot images to storage
    boxplot_path = os.path.dirname(__file__)
    boxplot_files = glob.glob(os.path.join(boxplot_path,"vmin_distribution_*"))
    print(f"Moving boxplot files...")
    for boxplot_file in boxplot_files:
        filename = os.path.basename(boxplot_file)
        new_path = os.path.join(data_folder_path, filename)
        move_file_with_retry(boxplot_file, new_path)



    print(f"All files have been moved to {data_folder_path}")
    logging.info(f"All files have been moved to {data_folder_path}")
    print("JOB DONE!")
    logging.info("JOB DONE!")
    print("---------------------------------------------------------------")
    logging.info('-------------------------------------------------------------------')

    # Call back first process
    next_script_path = './01_retrieve.py'
    os.system(f'python {next_script_path}')

except Exception as e:
    error_handling.handle_exception(e)