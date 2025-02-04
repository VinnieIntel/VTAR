import win32com.client
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import glob
import datetime
import logging
import time

wait_duration = 60 * 60  # One hour check one time

logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("============================================")
logging.info('Process 1: Retrieve information from email')
logging.info("============================================")

print("\n============================================")
print(f"Process 1: Retrieve information from email")
print("============================================")

# Delete previous files
files_to_delete_directory = os.path.dirname(__file__)
png_files = glob.glob(os.path.join(files_to_delete_directory, '*.png'))
xlsx_files = glob.glob(os.path.join(files_to_delete_directory, '*.xlsx'))
csv_files = glob.glob(os.path.join(files_to_delete_directory, 'lot_list_processed.csv'))

print(f"Deleting previous files ...")
logging.info('Deleting previous files ...')

all_files_to_delete = png_files + xlsx_files + csv_files
for all_file in all_files_to_delete:
    try:
        os.remove(all_file)
        print(f"Deleted file: {all_file}")
        logging.info(f"Deleted file: {all_file}")
    except OSError as e:
        print(f"Error: {all_file} : {e.strerror}")
        logging.info(f"Error: {all_file} : {e.strerror}")

# Perform info extract
def extract_info_from_html(html_content, lot="Lot", dfc_pattern=r"Vmin@[A-Z0-9]+@\d+\.\d+@(?:\d+)?", hdmx_cell_column_name="Hdmx Cell", unit_tester_id="Unit Tester Id"):
    soup = BeautifulSoup(html_content, "html.parser")

    result = {
        "lot_number": None,
        "domain_frequency_cores": [],
        "tool_names": [],
        "cell_names": [],
        "unit_tester_id": None
    }

    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        header_cells = rows[0].find_all(["th", "td"])

        headers = [cell.get_text(strip=True) for cell in header_cells]
        if lot in headers:
            lot_index = headers.index(lot)

            lot_row = rows[1] if len(rows) > 1 else None
            if lot_row:
                lot_column = lot_row.find_all("td")
                if lot_index < len(lot_column):
                    result["lot_number"] = lot_column[lot_index].get_text(strip=True)
                else:
                    print(f"Warning: 'Lot' column index ({lot_index}) is out of range for the row.")
                    logging.info(f"Warning: 'Lot' column index ({lot_index}) is out of range for the row.")

        if unit_tester_id in headers:
            tester_index = headers.index(unit_tester_id)

            tester_row = rows[1] if len(rows) > 1 else None
            if tester_row:
                tester_column = lot_row.find_all("td")
                if tester_index < len(tester_column):
                    result["unit_tester_id"] = tester_column[tester_index].get_text(strip=True)
                else:
                    print(f"Warning: 'Unit Tester Id' column index ({tester_index}) is out of range for the row.")
                    logging.info(f"Warning: 'Unit Tester Id' column index ({tester_index}) is out of range for the row.")

        if hdmx_cell_column_name in headers:
            hdmx_cell_index = headers.index(hdmx_cell_column_name)

            for row in rows[1:]:  # Skip the header row
                cells = row.find_all("td")
                if hdmx_cell_index < len(cells):
                    hdmx_cell_text = cells[hdmx_cell_index].get_text(strip=True)
                    # Match the pattern for tool and cell names
                    match = re.match(r"([A-Z]{3}\d{3})_([A-Z]\d{3})", hdmx_cell_text)
                    if match:
                        tool_name, cell_name = match.groups()
                        result["tool_names"].append(tool_name)
                        result["cell_names"].append(cell_name)

        for row in rows:
            cells = row.find_all("td")
            for cell in cells:
                cell_text = cell.get_text(strip=True)

                dfc_matches = re.findall(dfc_pattern, cell_text)
                for match in dfc_matches:
                    if match not in result["domain_frequency_cores"]:  # Check for uniqueness
                        result["domain_frequency_cores"].append(match)
    return result

def process_email():
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    messages = inbox.Items

    # (newest first)
    messages.Sort("[ReceivedTime]", True)

    # sender_email = "yasothye.muniandy@intel.com"
    sender_email ="vinnie.wen.ying.tiang@intel.com"
    subject_keyword = "HDMX_Vmin Check "

    email_found = False

    for message in messages:
        if message.Class == 43:  # only process mail item
            try:
                exch_sender = message.Sender
                sender_address = exch_sender.GetExchangeUser().PrimarySmtpAddress
            except AttributeError:
                sender_address = message.SenderEmailAddress
            if sender_address.lower() == sender_email.lower() and message.Subject.lower().startswith(subject_keyword.lower()):
                email_found = True
                print("\nEmail found (The Latest Trigger Email):")
                print(f"Subject: {message.Subject}")
                print(f"Sender: {message.SenderName}")
                print(f"Received: {message.ReceivedTime}")
                logging.info('*************************************************************')
                logging.info("Email found (The Latest Trigger Email):")
                logging.info(f"Subject: {message.Subject}")
                logging.info(f"Sender: {message.SenderName}")
                logging.info(f"Received: {message.ReceivedTime}")

                if message.BodyFormat == 2:
                    html_body = message.HTMLBody
                    info = extract_info_from_html(html_body)

                    if info["lot_number"]:
                        print(f"\nLot number: {info['lot_number']}\n")
                        logging.info(f"Lot number: {info['lot_number']}")
                    else:
                        print("Lot number not found.\n")
                        logging.info("Lot number not found.\n")

                    if info["domain_frequency_cores"]:
                        for i, token in enumerate(info["domain_frequency_cores"], start=1):
                            print(f"Domain Frequency Core {i}: {token}")
                            logging.info(f"Domain Frequency Core {i}: {token}")
                    else:
                        print("No tokens found under 'Domain Frequency Core'.\n")
                        logging.info("No tokens found under 'Domain Frequency Core'.\n")

                    if info["tool_names"]:
                        for i, name in enumerate(info["tool_names"], start=1):
                            print(f"Tool Name : {name}")
                            logging.info(f"Tool Name : {name}")
                    else:
                        print("No tool name found.\n")
                        logging.info("No tool name found.\n")

                    if info["cell_names"]:
                        for i, name in enumerate(info["cell_names"], start=1):
                            print(f"Cell Name : {name}")
                            logging.info(f"Cell Name : {name}")
                    else:
                        print("No cell name found.\n")
                        logging.info("No cell name found.\n")

                    if info["unit_tester_id"]:
                        print(f"Unit Tester Id: {info['unit_tester_id']}\n")
                        logging.info(f"Unit Tester Id: {info['unit_tester_id']}")
                    else:
                        print("Unit Tester Id not found.\n")
                        logging.info("Unit Tester Id not found.\n")

                else:
                    print("Email body is not in HTML format.")
                    logging.info("Email body is not in HTML format.")
                break

    if email_found:
        with open('email_entry_id.txt', 'r') as file:
            existing_entry_id = file.read().strip()

        if message.EntryID == existing_entry_id:
            print(f"{datetime.datetime.now()}: No new trigger email detected. :) \n")
            logging.info(f'No new trigger email detected. :) ')
            logging.info('*************************************************************************\n')
        else:
            with open('email_entry_id.txt', 'w') as file:
                file.write(message.EntryID)

            script_dir = os.path.dirname(__file__)
            output_csv_path = os.path.join(script_dir, 'output.csv')

            max_length = max(len(info['domain_frequency_cores']), len(info['tool_names']), len(info['cell_names']))
            data = {
                'Lot Number': [info['lot_number']] * max_length,
                'Domain Frequency Cores': info['domain_frequency_cores'] + [''] * (max_length - len(info['domain_frequency_cores'])),
                'Tool Names': info['tool_names'] + [''] * (max_length - len(info['tool_names'])),
                'Cell Names': info['cell_names'] + [''] * (max_length - len(info['cell_names'])),
                'Unit Tester Id': [info['unit_tester_id']] * max_length,
            }
            output_df = pd.DataFrame(data)
            output_df.to_csv('output.csv', index=False)
            print(f"Output saved to '{output_csv_path}'")
            logging.info(f"Output saved to '{output_csv_path}'")

            # Open folder for this triggering lot
            # data_folder = r"\\atdfile3.ch.intel.com\catts\export\csv\vtiang\Vmin_dummy\dummy_site\dummy_group" #Decided not to use this because uploading to atdfile3 consume alot of time
            df_info = pd.read_csv(output_csv_path)
            lot_number = df_info['Lot Number'].iloc[0]
            storage_folder = r"\\KMATSHFS.intel.com\KMATAnalysis$\MAOATM\PG\Personal\vtiang"
            data_folder_path = os.path.join(storage_folder, lot_number)
            print(data_folder_path)
            logging.info(data_folder_path)
            with open('data_folder_path.txt', 'w') as file:
                file.write(data_folder_path)

            # Call next process
            next_script_path = './02_lotlist.py'
            os.system(f'python {next_script_path}')
    else:
        print(f"No email found from {sender_email} with the subject '{subject_keyword}'.")
        logging.info(f"No email found from {sender_email} with the subject '{subject_keyword}'.")

while True:
    try:
        process_email()
        time.sleep(wait_duration)
    except KeyboardInterrupt:
        print("Process interrupted by user.")
        logging.info("Process interrupted by user.")
        break
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
        break