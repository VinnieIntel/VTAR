import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import shutil
import logging
import error_handling
import numpy as np
import sys
import subprocess

script_dir = os.path.dirname(__file__)
log_path = os.path.join(script_dir,'log_file.log')
info_path= os.path.join(script_dir,'output.csv')
the_lot_path = os.path.join(script_dir,"the_lot.csv")
limit_value_path = os.path.join(script_dir,'limit_value.csv')
data_folder_path_storage = os.path.join(script_dir,'data_folder_path.txt')
lot_list_path = os.path.join(script_dir,'lot_list_processed.csv')
lot_transaction_path = os.path.join(script_dir,'lot_transaction.csv')
process_07 =os.path.join(script_dir,"07_workingpath.py")
process_02A = os.path.join(script_dir,'02A_thelot_before.py')
process_02B = os.path.join(script_dir,'02B_thelot_after.py')
python_executable = sys.executable

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("==================================================")
logging.info('Process 6: Plot the Scatter Plot with limit line')
logging.info("==================================================")

print("\n=================================================")
print(f"Process 6: Plot the Scatter Plot with limit line")
print("=================================================")

try:
    # prepare data to plot
    df_info = pd.read_csv(info_path)
    vmin_values = df_info['Domain Frequency Cores'].unique().tolist()
    print(f"Vmin values : {vmin_values}")
    logging.info(f"Vmin values : {vmin_values}")

    vmin_prefixes = set(vmin_value.rsplit('@', 1)[0] for vmin_value in vmin_values)
    print(f"Vmin prefixes : {vmin_prefixes}")
    logging.info(f"Vmin prefixes : {vmin_prefixes}")

    df_csv = pd.read_csv(the_lot_path)
    hw_name = df_csv['DEVICE_TESTER_ID'].iloc[0]
    print(f"Device Tester ID: {hw_name}")
    logging.info(f"Device Tester ID: {hw_name}")

    limit_df = pd.read_csv(limit_value_path)
    x_iqr_limit = limit_df['x_iqr'].iloc[0]
    delta_p25_limit = limit_df['delta'].iloc[0]
    print (f"Limit for X_iqr: {x_iqr_limit}")
    logging.info (f"Limit for X_iqr: {x_iqr_limit}")
    print (f"Limit for delta_p25: {delta_p25_limit}\n")
    logging.info (f"Limit for delta_p25: {delta_p25_limit}")

    vmin_result_files_path = r"\\atdfile3.ch.intel.com\catts\export\csv\vtiang\Vmin_dummy\dummy_site\dummy_group\HIST"
    # vmin_result_files_path = r"Y:\Vmin_dummy\dummy_site\dummy_group\HIST"

    vmin_file_pattern = os.path.join(vmin_result_files_path, "vmin_result*.csv")

    vmin_files = glob.glob(vmin_file_pattern)
    if vmin_files:
        vmin_file = vmin_files[0] # I only expect to get one vmin_result file
        df = pd.read_csv(vmin_file)
        fac_lot_number = df['fac_lot'].iloc[0]
        lot_number = fac_lot_number.split('_')[-1]

        filtered_df = df[df['hw_type'] == "unit_tester_id"]
        filtered_df = filtered_df[filtered_df['x_iqr'] != 999]
        filtered_df_with_hw = filtered_df[filtered_df['hw_name'] == hw_name]
        filtered_df_other_hw = filtered_df[filtered_df['hw_name'] != hw_name]
        print (f'\n{fac_lot_number}')
        logging.info (f'{fac_lot_number}')
        print(filtered_df_with_hw.shape)
        logging.info(filtered_df_with_hw.shape)
        print(filtered_df_other_hw.shape)
        logging.info(filtered_df_other_hw.shape)

        plt.scatter(filtered_df_other_hw['delta_p25'], filtered_df_other_hw['x_iqr'], alpha=0.5)
        plt.scatter(filtered_df_with_hw['delta_p25'], filtered_df_with_hw['x_iqr'], color='red', alpha=0.5)

        plt.axhline(y=-x_iqr_limit, color='red', linestyle='--', label=f'X_iqr limit (-{x_iqr_limit})')  
        plt.axvline(x=-delta_p25_limit, color='red', linestyle='--', label=f'Delta_p25 limit (-{delta_p25_limit})')   

        plt.title(f'Bivariate Fit of x_iqr By delta_p25 fac_lot = {fac_lot_number}')
        plt.xlabel('delta_p25')
        plt.ylabel('x_iqr')
        plt.legend()
        plt.tight_layout() 

        plot_filename = f'Scatterplot_{fac_lot_number}.png'
        plt.savefig(plot_filename)
        plt.close()

        # Rename the vmin_result file to include the lot number
        # new_vmin_file_name = f"vmin_result_{os.path.basename(vmin_file).split('_')[1].split('.')[0]}_{fac_lot_number}.csv"
        new_vmin_file_name = f"vmin_result_{fac_lot_number}.csv"
        new_vmin_file_path = os.path.join(vmin_result_files_path, new_vmin_file_name)
        os.rename(vmin_file, new_vmin_file_path)
        vmin_file = new_vmin_file_path  # Update the vmin_file variable to the new file name


        ### Handle Downshift but within limit ###
        filtered_df = filtered_df[filtered_df['domain_frequency_core'].apply(lambda x: any(x.startswith(prefix) for prefix in vmin_prefixes))]  #filter vmin token
        filtered_df_with_hw_centroid = filtered_df[filtered_df['hw_name'] == hw_name]
        filtered_df_other_hw_centroid = filtered_df[filtered_df['hw_name'] != hw_name]
        with_hw_centroid = filtered_df_with_hw_centroid[['delta_p25', 'x_iqr']].mean().to_numpy()
        other_hw_centroid = filtered_df_other_hw_centroid[['delta_p25', 'x_iqr']].mean().to_numpy()
        plt.scatter(filtered_df_other_hw_centroid['delta_p25'], filtered_df_other_hw_centroid['x_iqr'], alpha=0.5)
        plt.scatter(filtered_df_with_hw_centroid['delta_p25'], filtered_df_with_hw_centroid['x_iqr'], color='red', alpha=0.5)
        plt.axhline(y=-x_iqr_limit, color='red', linestyle='--', label=f'X_iqr limit (-{x_iqr_limit})')  
        plt.axvline(x=-delta_p25_limit, color='red', linestyle='--', label=f'Delta_p25 limit (-{delta_p25_limit})')  
        plt.scatter(*with_hw_centroid, color='black', marker='x', s=100, label='With HW Centroid')
        plt.scatter(*other_hw_centroid, color='blue', marker='x', s=100, label='Other HW Centroid')
        print(f"Centroid of with_hw data: {with_hw_centroid}")
        print(f"Centroid of other_hw data: {other_hw_centroid}")
        # Step 2: Calculate the Euclidean distance between centroids
        vector_diff = with_hw_centroid - other_hw_centroid
        distance = np.linalg.norm(vector_diff)
        print(f"Distance between centroids: {distance}")
        plt.title(f'Bivariate Fit of x_iqr By delta_p25 fac_lot = {fac_lot_number}')
        plt.xlabel('delta_p25')
        plt.ylabel('x_iqr')
        plt.legend()
        plt.tight_layout() 
        plot_centroid_filename = f'Scatterplot_centroid_{fac_lot_number}.png'
        plt.savefig(plot_centroid_filename)
        plt.close()

        distance_threshold = 0.5  # Adjust here if needed
        if distance > distance_threshold:
            # Check if `with_hw_centroid` is strictly lower in both axes
            if vector_diff[0] < 0 and vector_diff[1] < 0:
                is_downshift_within_limit = True
                print(f"Downshift detected: Distance ({distance}) exceeds threshold ({distance_threshold}).")
            else:
                is_downshift_within_limit = False
                print("The shift is an upshift, not considered a downshift.")
        else:
            is_downshift_within_limit = False
            print(f"No downshift detected: Distance ({distance}) is within threshold ({distance_threshold}).")
    else:
        print("No vmin result files found!! Something is wrong...")
        logging.info("No vmin result files found!! Something is wrong...")

    # Check if any data points exceed the limits
    is_downshift = (
        (filtered_df_with_hw['delta_p25'] < -delta_p25_limit) |
        (filtered_df_with_hw['x_iqr'] < -x_iqr_limit)
    ).any()

    print(f"downshift status: {is_downshift}")

    if is_downshift:
        print(f"This lot is downshift by exceeding the limit.")
    else:
        print(f"This lot has not been detected to have datapoints exceeding the limit.")

    # # Move vmin file processed
    with open(data_folder_path_storage,'r') as file :
        data_folder_path = file.read().strip()

    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)
    
    filename = os.path.basename(vmin_file)
    new_path = os.path.join(data_folder_path, filename)
    shutil.move(vmin_file, new_path)

    df_lot_list = pd.read_csv(lot_list_path)
    print(f'Lot Number processing: {lot_number}')
    logging.info(f'Lot Number processing: {lot_number}')

    matching_row_index = df_lot_list.index[df_lot_list['CLS_LOT'] == lot_number].tolist()
    matching_row = df_lot_list[df_lot_list['CLS_LOT'] == lot_number]
    lot_sequence = matching_row['LOT_SEQUENCE'].iloc[0]
    
    #handle "in progress" lot
    df_transaction = pd.read_csv(lot_transaction_path)
    print(df_transaction['TRANSACTION'].values)
    if 'MVOU' not in df_transaction['TRANSACTION'].values:
        print(f"MVOU not found in TRANSACTION column. {lot_number} is in progress.")
        logging.info(f"MVOU not found in TRANSACTION column. {lot_number} is in progress.")
        df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'in progress'
        new_column_order = [
        'CLS_LOT', 'LOT_SEQUENCE', 'STATUS', 'CLS_OPERATION', 'CLS_TESTER_ID', 'CLS_THERMAL_HEAD', 
        'DEVICE_END_DATE_TIME', 'SITE_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID'
        ]
        rearranged_lot_list_df = df_lot_list[new_column_order]
        rearranged_lot_list_df.to_csv(lot_list_path, index=False)
        next_script_path = process_07
        subprocess.run([python_executable, next_script_path])
    else: 
        print(f"MVOU is found in TRANSACTION column. {lot_number} is completed.")
        if len(df_lot_list) == 1: # if the_lot current is N (first lot under process)
            if is_downshift:
                df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'downshift'
            elif is_downshift_within_limit:
                df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'downshift but within limit' # Most probably wont reach this line :)
            else:
                df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'clean'
            new_column_order = [
            'CLS_LOT', 'LOT_SEQUENCE', 'STATUS', 'CLS_OPERATION', 'CLS_TESTER_ID', 'CLS_THERMAL_HEAD', 
            'DEVICE_END_DATE_TIME', 'SITE_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID'
            ]
            rearranged_lot_list_df = df_lot_list[new_column_order]
            rearranged_lot_list_df.to_csv(lot_list_path, index=False)

            if is_downshift:
                print(f"Dataset for {fac_lot_number} is downshift.")
                logging.info(f"Dataset for {fac_lot_number} is downshift.")
                next_script_path = process_02A
            elif is_downshift_within_limit: # Most probably wont reach this line :)
                print(f"Dataset for {fac_lot_number} is downshift but within limit.")
                logging.info(f"Dataset for {fac_lot_number} is downshift but within limit.")
                next_script_path = process_02A
            else:    
                print(f"Dataset for {fac_lot_number} is clean.")
                logging.info(f"Dataset for {fac_lot_number} is clean.")
                next_script_path = process_02B
                
            subprocess.run([python_executable, next_script_path])
        else:
            matching_row_index = df_lot_list.index[df_lot_list['CLS_LOT'] == lot_number].tolist()
            # Find the row in lot_list_processed.csv that matches the current lot
            matching_row = df_lot_list[df_lot_list['CLS_LOT'] == lot_number]
            lot_sequence = matching_row['LOT_SEQUENCE'].iloc[0]

            if is_downshift:
                df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'downshift'
            elif is_downshift_within_limit:
                df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'downshift but within limit'
            else:
                df_lot_list.loc[matching_row_index[0], 'STATUS'] = 'clean'
                    
            new_column_order = [
            'CLS_LOT', 'LOT_SEQUENCE', 'STATUS', 'CLS_OPERATION', 'CLS_TESTER_ID', 'CLS_THERMAL_HEAD', 
            'DEVICE_END_DATE_TIME', 'SITE_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID'
            ]
            rearranged_lot_list_df = df_lot_list[new_column_order]
            rearranged_lot_list_df.to_csv(lot_list_path, index=False)

            if not matching_row.empty: # for debug error
                if is_downshift | is_downshift_within_limit:
                    if 'N-' in lot_sequence:
                        print(f"Dataset for {lot_number} is downshift, and in the past sequence.")
                        logging.info(f"Dataset for {lot_number} is downshift, and in the past sequence.")
                        next_script_path = process_02A
                    elif 'N+' in lot_sequence:
                        print(f"Dataset for {lot_number} is downshift, and in the future sequence.")
                        logging.info(f"Dataset for {lot_number} is downshift, and in the future sequence.")
                        next_script_path = process_02B
                    else: # for debug
                        raise ValueError(f"Unexpected LOT_SEQUENCE format: {lot_sequence}")
                
                else:
                    if 'N-' in lot_sequence:
                        # Handle if not enough data in a lot
                        if filtered_df_with_hw.shape[0] < 20:
                            print(f"Dataset for {fac_lot_number} is in the past sequence, it is clean but have not enough data (less than 20 data points). This lot have not enough cell data due to technical problem, skipping this cell...")
                            rearranged_lot_list_df.loc[matching_row_index[0], 'STATUS'] = 'clean but not enough data' 
                            rearranged_lot_list_df.to_csv(lot_list_path, index=False)
                            next_script_path = process_02A
                        else:
                            current_index = matching_row_index[-1]
                            previous_index = current_index - 1
                            current_lot_status = rearranged_lot_list_df.loc[current_index, 'STATUS']
                            previous_lot_status = rearranged_lot_list_df.loc[previous_index, 'STATUS']
                            print(f"Current Lot Status : {current_lot_status}")
                            print(f"Previous Lot Status : {previous_lot_status}")
                            logging.info(f"Current Lot Status : {current_lot_status}")
                            logging.info(f"Previous Lot Status : {previous_lot_status}")

                            # Ensure 2 clean lots are found
                            if previous_lot_status == 'clean':
                                print(f"Dataset for {lot_number} is in the past sequence, and it is clean for 2 lots... Proceed with lots in future sequence")
                                logging.info(f"Dataset for {lot_number} is in the past sequence, and it is clean for 2 lots... Proceed with lots in future sequence")
                                next_script_path = process_02B
                            else:
                                print(f"Dataset for {lot_number} is in the past sequence, first clean lot is found... One more continuous clean lot is needed")
                                logging.info(f"Dataset for {lot_number} is in the past sequence, first clean lot is found... One more continuous clean lot is needed")
                                next_script_path = process_02A

                    elif 'N+' in lot_sequence:
                        if filtered_df_with_hw.shape[0] < 20:
                            print(f"Dataset for {fac_lot_number} is in the future sequence, it is clean but have not enough data (less than 20 data points). This lot have not enough cell data due to technical problem, skipping this cell...")
                            rearranged_lot_list_df.loc[matching_row_index[0], 'STATUS'] = 'clean but not enough data' 
                            rearranged_lot_list_df.to_csv(lot_list_path, index=False)
                            next_script_path = process_02B
                        else:
                            print(f"Dataset for {lot_number} is in the future sequence, and it is clean... Processes are done!!")
                            logging.info(f"Dataset for {lot_number} is in the future sequence, and it is clean... Processes are done!!")
                            next_script_path = process_07
                    else: 
                        raise ValueError(f"Unexpected LOT_SEQUENCE format: {lot_sequence}")
                python_executable = sys.executable
                subprocess.run([python_executable, next_script_path])
            else:
                print(f"Current lot {lot_number} not found in {lot_list_path}.")
                logging.info(f"Current lot {lot_number} not found in {lot_list_path}.")

except Exception as e:
    error_handling.handle_exception(e)