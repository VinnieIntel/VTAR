import pandas as pd
import cx_Oracle
import warnings
import os
import logging
import error_handling
import time
import sys
import subprocess

warnings.filterwarnings("ignore")

script_dir = os.path.dirname(__file__)
log_path = os.path.join(script_dir,'log_file.log')
the_lot_path = os.path.join(script_dir,"the_lot.csv")
lot_transaction = os.path.join(script_dir,'lot_transaction.csv')
next_script_path = os.path.join(script_dir,'06_scatterplot.py')

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("===================================================")
logging.info('Process 5: Check the Transaction Status of the lot')
logging.info("===================================================")

print("\n=====================================================")
print(f"Process 5: Check the Transaction Status of the lot")
print("=====================================================")

try: 
    df_csv = pd.read_csv(the_lot_path)
    lot_number  = df_csv.loc[0,'LOT']
    operation = df_csv.loc[0,'OPERATION']

    def extract_database(lot_number, operation, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                con_aries = cx_Oracle.connect("", "", "PG.[A12_PROD_0.].MARS")
                sql_aries = f"""
                SELECT 
                    f0.lot AS lot
                    ,f0.operation AS operation
                    ,f5.transaction AS transaction
                FROM 
                A12_PROD_0.F_LOTHIST f0
                LEFT JOIN A12_PROD_0.F_LOTTXNHIST f5 ON f5.lot = f0.lot AND f5.operation = f0.operation AND f5.prevout_date = f0.prevout_date AND NVL(f5.history_deleted_flag,'N') = 'N'
                WHERE
                NVL(f0.history_deleted_flag,'N') = 'N'
                AND      f0.owner <> 'EMPTYFOUP'
                AND      (f0.lot LIKE  '{lot_number}'
                ) 
                AND      f0.operation = '{operation}' 
                -- Tail A
                """

                df_aries = pd.read_sql(sql_aries, con_aries)

                df_aries.to_csv(lot_transaction, index=False)
                print(f"Status of {lot_number} saved to '{lot_transaction}'") 
                logging.info(f"Status of {lot_number} saved to '{lot_transaction}'") 
                    
                print(df_aries.to_string(index=False))
                logging.info(f"Contents of {lot_transaction}:\n{df_aries.to_string(index=False)}") 
                
                return  # Exit the function if successful

            except Exception as e:
                logging.error(f"Error Retrieving Data of Lot Transaction: {e}")
                print(f"Error Retrieving Data of Lot Transaction: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print("Retrying in 5 minutes...")
                    time.sleep(300)  # Wait for 5 minutes
                else:
                    print("Maximum retries reached. Bypassing this step.")
                    logging.error("Maximum retries reached. Bypassing this step.")
                    return  # Exit the function after max retries

    extract_database(lot_number, operation)

    # Call next process
    python_executable = sys.executable
    subprocess.run([python_executable, next_script_path])

except Exception as e:
    error_handling.handle_exception(e)