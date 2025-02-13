import os 
import datetime
import pandas as pd
import logging
import glob
import shutil
import time
import error_handling
import sys
import subprocess

script_dir = os.path.dirname(__file__)
log_path = os.path.join(script_dir,'log_file.log')
lot_csv_path = os.path.join(script_dir,'the_lot.csv')
SPFSQL_File_path =os.path.join(script_dir,'vmin_norobocopy.spfsql')
next_script_path = os.path.join(script_dir,'04_limit.py')

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("====================================================")
logging.info('Process 3: SQLPathFinder Python API to Get 3 Files')
logging.info("====================================================")

print("\n===================================================")
print(f"Process 3: SQLPathFinder Python API to Get 3 Files")
print("===================================================")

try:
    df_csv = pd.read_csv(lot_csv_path)
    the_lot = df_csv.loc[0,'LOT']
    print(f'The lot: {the_lot}\n')
    logging.info(f'*********************')
    logging.info(f'* The lot: {the_lot} *')
    logging.info(f'*********************')

    # ########### Get Latest Version of the API ##############
    from subprocess import call
    print("============================================================================")
    print("Verifying whether your local SQLPFSvcClient API is up to date")
    print("============================================================================")
    logging.info("Verifying whether your local SQLPFSvcClient API is up to date")

    HPC_API_Dir = r"\\atdfile3.ch.intel.com\atd-web\PathFinding\SQLPathFinder\Python_Packages\SPF_HPC"
    HPC_API_File = "SQLPFSvcClient.py"
    try:
        call(["robocopy", HPC_API_Dir, script_dir, HPC_API_File, "/R:100", "/W:30", "/NP", "/IS", "/S", "/XO"])
    except:
        pass

    ###### Prepare Required files in atdfile environment #######
    print("=================================================")
    print("Preparing Required files in atdfile environment")
    print("=================================================")
    logging.info('Preparing Required files in atdfile environment...')
    required_files_path = os.path.join(os.path.dirname(__file__),"required_files")
    working_path = r"\\atdfile3.ch.intel.com\catts\export\csv\vtiang\Vmin_dummy"

    all_files = glob.glob(os.path.join(required_files_path, "*"))
    required_files_copied = False  

    for file_path in all_files:
        filename = os.path.basename(file_path)
        new_path = os.path.join(working_path, filename)
        try:
            shutil.copy(file_path, new_path)
            print(f"Copied {filename} to {new_path}")
            logging.info(f"Copied {filename} to {new_path}")
            required_files_copied = True
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            logging.error(f"File not found: {file_path}")
        except PermissionError:
            print(f"Permission denied: {file_path}")
            logging.error(f"Permission denied: {file_path}")
        except Exception as e:
            print(f"Error copying {file_path}: {e}")
            logging.error(f"Error copying {file_path}: {e}")

    if required_files_copied:
        print(f"Required files are ready in {working_path}")
        logging.info(f"Required files are ready in {working_path}...")
    else:
        print(f"** Warning: Required files are NOT copied into atdfile!! Script might not run...")
        logging.info(f"** Warning: Required files are NOT copied into atdfile!! Script might not run... Please make sure atdfile3 directory (Working Path) is correct.")

    ######### IMPORTING ASYNC API ##############
    from SQLPFSvcClient import SQLPFSvcClient  # Async API
    import os
    print("=================================================")
    print("Starting retrieval of the 3 vmin files from SQLPF")
    print("=================================================")
    logging.info('Starting retrieval of the 3 vmin files from SQLPF')

    # ###### Execution of sqpfql file with In Group Filter ###############

    def delete_hist_files(the_lot):
        hist_folder = r"\\atdfile3.ch.intel.com\catts\export\csv\vtiang\Vmin_dummy\dummy_site\dummy_group\HIST"  # Replace with the actual path to the HIST folder
        files_to_check = glob.glob(os.path.join(hist_folder, '*'))

        for file_path in files_to_check:
            try:
                # Attempt to read the file as a CSV
                df = pd.read_csv(file_path)
                if 'fac_lot' in df.columns:
                    # Extract the lot number part after the underscore
                    df['lot_number'] = df['fac_lot'].apply(lambda x: x.split('_')[1] if '_' in x else '')
                    if the_lot in df['lot_number'].values:
                        os.remove(file_path)
                        print(f"Deleted {file_path}")
                        logging.info(f"Deleted {file_path}")
                    else:
                        print(f"File {file_path} does not contain the lot number {the_lot}")
                        logging.info(f"File {file_path} does not contain the lot number {the_lot}")
                else:
                    print(f"File {file_path} does not contain the 'fac_lot' column")
                    logging.info(f"File {file_path} does not contain the 'fac_lot' column")
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                logging.error(f"Error processing {file_path}: {e}")

    def execute_spf_client(max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                spf_client = SQLPFSvcClient(SPFSQL_File=SPFSQL_File_path,
                                            SPF_CL_Args={'CL_LOTNUMBER': the_lot}) 

                spf_client.Execute()
                return  # Exit the function if successful
            
            except PermissionError as e:
                logging.error(f"Permission error executing SQLPFSvcClient: {e}")
                print(f"Permission error executing SQLPFSvcClient: {e}")
                raise e  # Raise the exception to propagate it
            
            except Exception as e:
                logging.error(f"Error executing SQLPFSvcClient: {e}")
                print(f"Error executing SQLPFSvcClient: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print("Retrying in 5 minutes...")
                    time.sleep(300)  # Wait for 5 minutes
                    delete_hist_files(the_lot)
                else:
                    print(f"Maximum retries reached. Error executing SQLPFSvcClient: {e}")
                    logging.error(f"Maximum retries reached. Error executing SQLPFSvcClient: {e}")
                    raise e  # Raise the exception to propagate it

    execute_spf_client()

    
    try:
        python_executable = sys.executable
        subprocess.run([python_executable, next_script_path])

    except Exception as e:
        logging.error(f"Error executing next script {next_script_path}: {e}")
        print(f"Error executing next script {next_script_path}: {e}")
        print("Retrying in 5 minutes...")
        time.sleep(300)  # Wait for 5 minutes
        python_executable = sys.executable
        subprocess.run([python_executable, next_script_path]) # Retry

except Exception as e:
    error_handling.handle_exception(e)