import pandas as pd
import glob
import os
import seaborn as sns
import matplotlib.pyplot as plt
import logging
import error_handling

logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("==========================================")
logging.info('Process 8: Plot the boxplot by Tool Cells')
logging.info("==========================================")

print("\n============================================")
print(f"Process 8: Plot the boxplot by Tool Cells")
print("============================================")

try: 
    lot_list_path = 'lot_list_processed.csv'
    lot_list_df = pd.read_csv(lot_list_path)
    lot_list = lot_list_df['CLS_LOT'].tolist()[::-1] # make the boxplot goes in ascending order
    print(f'Lot List Processed: {lot_list}')
    logging.info(f'Lot List Processed: {lot_list}')

    # Get vmin values to plot
    output_df = pd.read_csv('output.csv')
    vmin_values = output_df['Domain Frequency Cores'].unique().tolist()
    print(f"Vmin values : {vmin_values}")
    logging.info(f"Vmin values : {vmin_values}")

    print("\nThis process will take some time... Please wait...\n")
    logging.info("This process will take some time... Please wait...")

    vmin_files_path = os.path.join(os.path.dirname(__file__),"VminFilesPlot")
    # vmin_files_path = "X:\VminFilesPlot" # This is for vinnie's own PC only, please use the above line for server

    vmin_files = glob.glob(os.path.join(vmin_files_path, "Vmin_*.csv"))
    lot_to_vmin_file_map = {}

    # map vmin file to its lot number
    for vmin_file in vmin_files:
        print(vmin_file)
        logging.info(vmin_file)
        try:
            lot_number = os.path.basename(vmin_file).split('_')[2].split('.')[0]
            lot_to_vmin_file_map[lot_number] = vmin_file
        except (pd.errors.EmptyDataError, IndexError):
            print(f"Warning: The file {vmin_file} is empty or does not contain expected data.")
            logging.info(f"Warning: The file {vmin_file} is empty or does not contain expected data.")

    # Sort the lot numbers and get the corresponding file paths
    sorted_lots = sorted(lot_to_vmin_file_map.keys())
    paired_vmin_files = [lot_to_vmin_file_map[lot] for lot in sorted_lots]

    print("Sorted Vmin files:")
    logging.info("Sorted Vmin files:")
    for lot, file in zip(sorted_lots, paired_vmin_files):
        print(f"{lot}: {file}")
        logging.info(f"{lot}: {file}")

    tool_name = output_df['Tool Names'].iloc[0]
    cell_name = output_df['Cell Names'].iloc[0]
    highlight_hdmx_cell = tool_name + '_' + cell_name
    print(f"Highlight HDMX Cell: {highlight_hdmx_cell}")
    logging.info(f"Highlight HDMX Cell: {highlight_hdmx_cell}")

    for vmin_value in vmin_values:
        # Create a figure with subplots (one for each lot)
        fig, axs = plt.subplots(1, len(paired_vmin_files), figsize=(8 * len(paired_vmin_files), 8), sharey=True) 

        def plot_lot(ax, df, lot_name):
            filtered_df = df[df['domain_frequency_core'] == vmin_value]
            filtered_df = filtered_df.sort_values(by='hdmx_cell')
            filtered_df = filtered_df[filtered_df['vmin'] != -9999]
            filtered_df['domain_frequency_core'] = pd.to_numeric(filtered_df['domain_frequency_core'], errors='coerce')
            boxplot = sns.boxplot(ax=ax, x='hdmx_cell', y='vmin', data=filtered_df, order=sorted(filtered_df['hdmx_cell'].unique()))
            
            # Highlight the triggering cell in red
            unique_hdmx_cells = sorted(filtered_df['hdmx_cell'].unique())
            for patch, label in zip(boxplot.patches, unique_hdmx_cells):
                logging.info(f"Label: {label}")
                if label == highlight_hdmx_cell:
                    patch.set_facecolor('red')
                    patch.set_alpha(0.5)

            sns.stripplot(ax=ax, x='hdmx_cell', y='vmin', data=filtered_df, color='black', size=5, jitter=True, order=sorted(filtered_df['hdmx_cell'].unique()))
            ax.set_title(f'{lot_name}')
            test_end_date = df['test_end_date'].iloc[0]
            ax.set_xlabel(test_end_date)
            if ax is axs[0]:
                ax.set_ylabel('Vmin Values')
            else:
                ax.set_ylabel('') 
            ax.tick_params(axis='x', rotation=90)

        # Loop each lot in the lot_list and plot
        for ax, lot in zip(axs, lot_list):
            vmin_file = lot_to_vmin_file_map.get(lot)
            if vmin_file:
                df = pd.read_csv(vmin_file)
                plot_lot(ax, df, lot)
            else:
                print(f"Warning: No Vmin file found for {lot}")
                logging.info(f"Warning: No Vmin file found for {lot}")
                
        fig.suptitle(f'Combined Vmin Distribution for {vmin_value}', fontsize=16)
        plt.tight_layout()
        plt.savefig(f'vmin_distribution_for_{vmin_value}'.replace('@', '_').replace('.', '_'))
        plt.close()

    print("-----DONE-----\n\n\n\n\n")
    logging.info("-----DONE-----")

    #Call next process
    next_script_path = './09_reply.py'
    os.system(f'python {next_script_path}')

except Exception as e:
    error_handling.handle_exception(e)