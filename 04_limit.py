import pandas as pd
import os
import datetime
import logging
import error_handling
import sys
import subprocess

script_dir = os.path.dirname(__file__)
log_path = os.path.join(script_dir,'log_file.log')
lot_csv_path = os.path.join(script_dir,'the_lot.csv')
limit_csv_path = os.path.join(script_dir,'find_limit.csv')
next_script_path = os.path.join(script_dir,'05_checkTransaction.py')
limit_value_path = os.path.join(script_dir,'limit_value.csv')


logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("====================================")
logging.info('Process 4: Get the limit for the TP')
logging.info("====================================")

print("\n======================================")
print(f"Process 4: Get the limit for the TP")
print("======================================")

try: 
    df_csv = pd.read_csv(lot_csv_path)
    lot_number  = df_csv.loc[0,'LOT']

    program_name = df_csv['PROGRAM_NAME'].iloc[0]
    print(f"Lot Number: {lot_number}") 
    logging.info(f"Lot Number: {lot_number}") 
    print(f"Program Name: {program_name}")
    logging.info(f"Program Name: {program_name}")

    df_limit = pd.read_csv(limit_csv_path)

    # Filter the rows with A01 for Site_list
    filtered_find_limit = df_limit[df_limit['Site_list'].str.contains('A01')]
    8
    # Filter rows TP matches the pattern from program_name
    matching_row = filtered_find_limit[filtered_find_limit['TP'].str.slice(0, -1).apply(lambda tp: program_name.startswith(tp))]

    if not matching_row.empty:
        x_iqr_value = matching_row['x_iqr'].tolist()
        delta_value = matching_row['delta'].tolist()

        df_limit_for_row = pd.DataFrame({
            'x_iqr': x_iqr_value,
            "delta": delta_value
        })

        print (f"X_iqr: {x_iqr_value}")
        logging.info (f"X_iqr: {x_iqr_value}")
        print (f"p25_delta: {delta_value}")
        logging.info(f"p25_delta: {delta_value}")
        df_limit_for_row.to_csv(limit_value_path,index=False)
        print(f"Output saved to '{limit_value_path}'")
        logging.info(f"Output saved to '{limit_value_path}'")

    else:
        print("No mathching program names found in the limit file list")
        logging.info("No mathching program names found in the limit file list")

    # Call next process
    python_executable = sys.executable
    subprocess.run([python_executable, next_script_path])
    
except Exception as e:
    error_handling.handle_exception(e)