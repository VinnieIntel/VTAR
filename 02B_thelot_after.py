import pandas as pd
import os
import logging
import error_handling
import sys
import subprocess

script_dir = os.path.dirname(__file__)
log_path = os.path.join(script_dir,'log_file.log')
the_lot_path = os.path.join(script_dir,"the_lot.csv")
full_lot_list_path = os.path.join(script_dir,'full_lot_list.csv')
processed_lot_list_path = os.path.join(script_dir,'lot_list_processed.csv')
next_script_path = os.path.join(script_dir,'03_SQLPF.py')

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("========================================================================")
logging.info('Process 2B: Update Processed Lot List & New Lot Number to be process (+)')
logging.info("========================================================================")

print("\n=======================================================================")
print(f"Process 2B: Update Processed Lot List & New Lot Number to be process (+)")
print("========================================================================")

try:
    df_full = pd.read_csv(full_lot_list_path)

    # Find the current lot
    df_processed = pd.read_csv(processed_lot_list_path)
    the_lot = df_processed['CLS_LOT'].iloc[0]
    print(f'Processed: {the_lot}')
    logging.info(f'Processed: {the_lot}')

    lot_index = df_full.index[df_full['LOT'] == the_lot].tolist()

    if lot_index and (lot_index[0] + 1) < len(df_full):
        next_lot_index = lot_index[0] + 1
        next_lot_row = df_full.iloc[[next_lot_index]]  

        next_lot_row.to_csv(the_lot_path, index=False)
        print(f"The next line after LOT {the_lot} has been saved to {the_lot_path}.\n")
        logging.info(f"The next line after LOT {the_lot} has been saved to {the_lot_path}.")
    else:
        print(f"LOT {the_lot} is the last entry or not found in the full lot list.")
        logging.info(f"LOT {the_lot} is the last entry or not found in the full lot list.")




    # Find the coming lot
    df_the_lot = pd.read_csv(the_lot_path)
    the_lot = df_the_lot.loc[0, 'LOT']

    # Update the processed lot list CSV file
    df_processed = pd.read_csv(processed_lot_list_path)

    # Determine LOT_SEQUENCE for the new row
    first_sequence = df_processed['LOT_SEQUENCE'].iloc[0]
    if first_sequence.startswith('N'):
        # Extract the number part (if any) and increment it
        number_part = first_sequence.split('N')[-1]
        if number_part.startswith('+') or number_part.startswith('-'):
            # If there is a number part with a sign, we convert it to an integer, increment it, and format it
            new_sequence_number = int(number_part) + 1
            new_sequence = f'N{new_sequence_number:+}'  # Use a plus sign for positive numbers
        else:
            # If there is no number part, this is the first sequence 'N'
            new_sequence = 'N+1'
    else:
        raise ValueError('Unexpected sequence format')


    new_column_order = [
        'LOT', 'OPERATION', 'MODULE_ID', 'THERMAL_HEAD_ID', 
        'DEVICE_END_DATE_TIME', 'CELL_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID'
    ]
    column_mapping = {
        'LOT': 'CLS_LOT',
        'OPERATION': 'CLS_OPERATION',
        'MODULE_ID': 'CLS_TESTER_ID',
        'THERMAL_HEAD_ID': 'CLS_THERMAL_HEAD',
        'DEVICE_END_DATE_TIME': 'DEVICE_END_DATE_TIME',
        'CELL_ID': 'SITE_ID',
        'TIU_PERSONALITY_CARD_ID': 'TIU_PERSONALITY_CARD_ID',
        'DEVICE_TESTER_ID': 'DEVICE_TESTER_ID'
    }


    new_column_order = ['CLS_LOT', 'LOT_SEQUENCE', 'STATUS','CLS_OPERATION', 'CLS_TESTER_ID', 'CLS_THERMAL_HEAD', 
                        'DEVICE_END_DATE_TIME', 'SITE_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID']

    # Prepare the new row from the_lot.csv with the correct LOT_SEQUENCE
    df_the_lot['LOT_SEQUENCE'] = new_sequence
    df_the_lot['STATUS'] =''
    df_the_lot = df_the_lot.rename(columns=column_mapping)
    new_row = df_the_lot[new_column_order]

    # Prepend the new row to the processed lot list DataFrame
    df_processed = pd.concat([new_row, df_processed]).reset_index(drop=True)

    # Save the updated DataFrame back to lot_list_processed.csv
    df_processed.to_csv(processed_lot_list_path, index=False)
    print(f'To process next: {the_lot} ({new_sequence})')
    logging.info(f'To process next: {the_lot} ({new_sequence})')
    print(f"The new line for LOT {the_lot} with sequence {new_sequence} has been prepended to {processed_lot_list_path}.\n")
    logging.info(f"The new line for LOT {the_lot} with sequence {new_sequence} has been prepended to {processed_lot_list_path}.")
    
    # Call next process
    python_executable = sys.executable
    subprocess.run([python_executable, next_script_path])
    
except Exception as e:
    error_handling.handle_exception(e)