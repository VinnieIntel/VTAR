import pandas as pd
import os
import datetime
import logging
import error_handling

logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("========================================================================")
logging.info('Process 2A: Update Processed Lot List & New Lot Number to be process (-)')
logging.info("========================================================================")

print("\n========================================================================")
print(f"Process 2A: Update Processed Lot List & New Lot Number to be process (-)")
print("========================================================================")

try:
    full_lot_list_path = 'full_lot_list.csv'
    df_full = pd.read_csv(full_lot_list_path)

    the_lot_path = "the_lot.csv"

    # Find the current lot
    processed_lot_list_path = 'lot_list_processed.csv'
    df_processed = pd.read_csv(processed_lot_list_path)
    the_lot = df_processed['CLS_LOT'].iloc[-1]
    print(f'Processed: {the_lot}')
    logging.info(f'Processed: {the_lot}')

    lot_index = df_full.index[df_full['LOT'] == the_lot].tolist()

    if lot_index and (lot_index[0] - 1) < len(df_full):
        next_lot_index = lot_index[0] - 1
        next_lot_row = df_full.iloc[[next_lot_index]]  

        next_lot_row.to_csv(the_lot_path, index=False)
        print(f"The next line before LOT {the_lot} has been saved to {the_lot_path}.\n")
        logging.info(f"The next line before LOT {the_lot} has been saved to {the_lot_path}.")
    else:
        print(f"LOT {the_lot} is the last entry or not found in the full lot list.")
        logging.info(f"LOT {the_lot} is the last entry or not found in the full lot list.")




    # Find the coming lot
    df_the_lot = pd.read_csv(the_lot_path)
    the_lot = df_the_lot.loc[0, 'LOT']

    # Update the processed lot list CSV file
    processed_lot_list_path = 'lot_list_processed.csv'
    df_processed = pd.read_csv(processed_lot_list_path)

    # Determine the LOT_SEQUENCE for the new row
    if not df_processed.empty:
        last_sequence = df_processed['LOT_SEQUENCE'].iloc[-1]
        if last_sequence.startswith('N'):
            # extract the number part (if any) and decrement it
            number_part = last_sequence.split('N')[-1]
            if number_part:
                # If there is a number part, we convert it to an integer, decrement it, and format it
                new_sequence_number = int(number_part) - 1
                new_sequence = f'N{new_sequence_number}'
            else:
                # If there is no number part, this is the first sequence 'N'
                new_sequence = 'N-1'
        else:
            raise ValueError('Unexpected sequence format')
    else:
        # If the processed file is empty, start with 'N'
        new_sequence = 'N'

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


    new_column_order = ['CLS_LOT', 'LOT_SEQUENCE', 'CLS_OPERATION', 'CLS_TESTER_ID', 'CLS_THERMAL_HEAD', 
                        'DEVICE_END_DATE_TIME', 'SITE_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID']

    # Prepare the new row from the_lot.csv with the correct LOT_SEQUENCE
    df_the_lot['LOT_SEQUENCE'] = new_sequence
    df_the_lot = df_the_lot.rename(columns=column_mapping)
    new_row = df_the_lot[new_column_order]

    # Append the new row to the processed lot list DataFrame
    df_processed = pd.concat([df_processed, new_row], ignore_index=True)

    # Save the updated DataFrame back to lot_list_processed.csv
    df_processed.to_csv(processed_lot_list_path, index=False)
    print(f'To process next: {the_lot} ({new_sequence})')
    logging.info(f'To process next: {the_lot} ({new_sequence})')
    print(f"The new line for LOT {the_lot} with sequence {new_sequence} has been appended to {processed_lot_list_path}.\n")
    logging.info(f"The new line for LOT {the_lot} with sequence {new_sequence} has been appended to {processed_lot_list_path}.")




    # Call next process
    next_script_path = './03_SQLPF.py'
    os.system(f'python {next_script_path}')

except Exception as e:
    error_handling.handle_exception(e)