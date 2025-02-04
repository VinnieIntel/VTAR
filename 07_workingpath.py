import glob
import os
import shutil
import logging
import traceback
import error_handling
import pandas as pd

logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("=================================================")
logging.info('Process 7: Move Vmin Files to Local Environment')
logging.info("=================================================")

print("\n===============================================")
print(f"Process 7: Move Vmin Files to Local Environment")
print("=================================================")

print(f"LOADING......")
logging.info('LOADING......')

try:
    ori_path = r"Y:\Vmin_dummy\dummy_site\dummy_group\HIST"
    vmin_files_to_plot = glob.glob(os.path.join(ori_path, "Vmin_2*"))
    for vmin_file in vmin_files_to_plot:
        try:
            df = pd.read_csv(vmin_file, nrows=5)
            lot_number = df['fac_lot'].iloc[0]
            new_vmin_file_name = f"Vmin_{lot_number}.csv"
            new_vmin_file_path = os.path.join(ori_path, new_vmin_file_name)
            os.replace(vmin_file, new_vmin_file_path)
        except (pd.errors.EmptyDataError, IndexError):
            print(f"Warning: The file {vmin_file} is empty or does not contain expected data.")
            logging.info(f"Warning: The file {vmin_file} is empty or does not contain expected data.")
        except FileExistsError:
            print(f"File already exists: {new_vmin_file_path}. Skipping renaming.")
            logging.info(f"File already exists: {new_vmin_file_path}. Skipping renaming.")
        except Exception as e:
            print(f"Error processing {vmin_file}: {e}")
            logging.error(f"Error processing {vmin_file}: {e}")


    vmin_files_to_plot = glob.glob(os.path.join(ori_path, "Vmin_*"))
    local_path = os.path.join(os.path.dirname(__file__),"VminFilesPlot")
    # local_path = "X:\VminFilesPlot" # This is for vinnie's own PC only, please use the above line for server

    os.makedirs(local_path, exist_ok=True)
    # Copy files and log progress
    i = 1
    for file_path in vmin_files_to_plot:
        filename = os.path.basename(file_path)
        try:
            shutil.copy(file_path, local_path)
            print(f"{i} file(s) copied: {filename}")
            logging.info(f"{i} file(s) copied: {filename}")
            i += 1
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            logging.error(f"File not found: {file_path}")
        except PermissionError:
            print(f"Permission denied: {file_path}")
            logging.error(f"Permission denied: {file_path}")
        except Exception as e:
            print(f"Error copying {file_path}: {e}")
            logging.error(f"Error copying {file_path}: {e}")


    print(f"\n\nVmin files to plot boxplots have been copied to {local_path} for further process")
    logging.info(f"Vmin files to plot boxplots have been copied to {local_path} for further process")
    print("--------------------------------------------------------------------------------\n\n")


    print("\n=====================================")
    print(f"Move Backup Scatterplots to Storage")
    print("=======================================")
    logging.info("\n=====================================")
    logging.info(f"Move Backup Scatterplots to Storage")
    logging.info("=======================================")

    with open('data_folder_path.txt', 'r') as file:
        data_folder_path = file.read().strip()
    
    scatterplot_files = glob.glob(os.path.join(os.path.dirname(__file__),"Scatterplot_centroid_*"))
    print(f"LOADING......")
    logging.info('LOADING......')

    for scatterplot_file in scatterplot_files:
        try:
            destination_path = os.path.join(data_folder_path, os.path.basename(scatterplot_file))
            if os.path.exists(destination_path):
                base, ext = os.path.splitext(destination_path)
                counter = 1
                new_destination_path = f"{base}_{counter}{ext}"
                while os.path.exists(new_destination_path):
                    counter += 1
                    new_destination_path = f"{base}_{counter}{ext}"
                destination_path = new_destination_path
            shutil.move(scatterplot_file, destination_path)
            print(f"Moved {scatterplot_file} to {destination_path}")
        except Exception as e:
            print(f"Failed to move {scatterplot_file}: {e}")
    

    # Call next process
    next_script_path = './08_plotting.py'
    os.system(f'python {next_script_path}')

except Exception as e:
    error_handling.handle_exception(e)
    
