import win32com.client as client
import os
import pandas as pd
import logging
import error_handling
import sys
import subprocess

script_dir = os.path.dirname(__file__)
log_path = os.path.join(script_dir,'log_file.log')
lot_list_path = os.path.join(script_dir,'lot_list_processed.csv')
email_id_path = os.path.join(script_dir,'email_entry_id.txt')
lot_csv_path = os.path.join(script_dir,'output.csv')
next_script_path = os.path.join(script_dir,'10_moveFiles.py')

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("=======================================")
logging.info('Process 9: Send Respond Email Back...')
logging.info("=======================================")

print("\n============================================")
print(f"Process 9: Send Respond Email Back...")
print("============================================")

try:
    image_dir = script_dir
    image_files = sorted(os.listdir(image_dir))

    # Get lot list
    lot_list_df = pd.read_csv(lot_list_path)
    lot_list = lot_list_df['CLS_LOT'].tolist()
    print(f"Lot List : {lot_list}")
    logging.info(f"Lot List : {lot_list}")

    annotations = lot_list_df['LOT_SEQUENCE'].to_list()
    print(f'Annotations: {annotations}')
    logging.info(f'Annotations: {annotations}')
    annotations.reverse()

    firstContLot =  annotations[2]
    lastContLot = annotations[-2]

    print(f'Containment From :{firstContLot} to {lastContLot} (+ any lot in progress)')
    logging.info(f'Containment From :{firstContLot} to {lastContLot} (+ any lot in progress)')

    with open(email_id_path, 'r') as file:
        email_entry_id = file.read().strip()
    outlook = client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    mail_item = outlook.GetItemFromID(email_entry_id)

    reply = mail_item.ReplyAll()  
    reply.To = "vinnie.wen.ying.tiang@intel.com"

    df_csv = pd.read_csv(lot_csv_path)
    lot_number = df_csv['Lot Number'].iloc[0]
    first_html_body = f"<html><body><b>Triggering Lot {lot_number}. This is a reply email with embedded scatterplot and boxplot images.</b><br><br></body>"

    lot_list_html_table = lot_list_df.to_html(index=False, border=6, classes='lot-list')
    html_body = f"<body><b>Window Time:</b><br><p>Containment From {firstContLot} to {lastContLot} (+ any lot in progress)</p><br><br> {lot_list_html_table}<br><br></body>"
    html_body = first_html_body + html_body

    html_words = '<br> Scatter plot: <br>'
    html_body += html_words

    html_body += "<body>"
    html_body += "<table>"
    sorted_scatterplot_files = []
    for lot in lot_list:
        for file in os.listdir(image_dir):
            if file.lower().endswith(f"{lot.lower()}.png") or file.lower().endswith(f"{lot.lower()}.jpg") or file.lower().endswith(f"{lot.lower()}.jpeg"):
                sorted_scatterplot_files.append(file)
    print(f"Scatter Plots: {sorted_scatterplot_files}")
    logging.info(f"Scatter Plots: {sorted_scatterplot_files}")
    sorted_scatterplot_files.reverse()

    for i in range(0, len(sorted_scatterplot_files), 3):
        html_body += "<tr>"  
        for j in range(3):  
            if i + j < len(sorted_scatterplot_files):
                scatterplot_file = sorted_scatterplot_files[i + j]
                annotation = annotations[i + j]
                attachment_path = os.path.join(image_dir, scatterplot_file)
                attachment = reply.Attachments.Add(attachment_path)
                attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001E", f"cid:{scatterplot_file}")
                # Add a cell to the row with the image and annotation
                html_body += f'''
                    <td>
                        <div style="text-align: center; color: red; font-size: 30px">
                            <b>{annotation}</b>
                        </div>
                        <div>
                            <img src="cid:{scatterplot_file}" width="450">
                        </div>
                    </td>'''
        html_body += "</tr>"  

        if i + 3 < len(sorted_scatterplot_files):
            html_body += "<tr><td colspan='3' style='height: 20px;'></td></tr>"  # Spacer row

    html_body += "</table>"

    sorted_boxplot_files = []
    for file in os.listdir(image_dir):
        if file.startswith("vmin_distribution") and file.lower().endswith(".png"):
            sorted_boxplot_files.append(file)
    print (f"Box Plots: {sorted_boxplot_files}")
    logging.info (f"Box Plots: {sorted_boxplot_files}")

    html_words = '<br> VMIN plot: <br>'
    html_body += html_words

    for boxplot_file in sorted_boxplot_files:
        if boxplot_file.lower().endswith(('.png', '.jpg', '.jpeg')):
            attachment_path2 = os.path.join(image_dir, boxplot_file)
            attachment2 = reply.Attachments.Add(attachment_path2)
            attachment2.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001E", f"cid:{boxplot_file}")
            html_body += f'<p><br><img src="cid:{boxplot_file}" ></p><br>'

    html_words = 'Vmin Files Directory Attached below:<br>'
    html_body += html_words

    # Convert the directory path to a file URL
    with open('data_folder_path.txt','r') as file :
        vmin_files_dir = file.read().strip()
    vmin_files_url = vmin_files_dir

    # Create an HTML link for the directory
    html_body += f'<a href="{vmin_files_url}">{vmin_files_dir}</a>'
    html_body += "</body></html>"

    reply.HTMLBody = html_body + reply.HTMLBody
    reply.Save()
    reply.Send()

    # reply.BodyFormat = 2  
    # original_html_body = mail_item.HTMLBody
    # reply.HTMLBody = html_body + original_html_body
    # reply.Save()
    # reply.Send()

    print("\nPlease wait for 1 minute...")
    logging.info("Please wait for 1 minute...")
    print("Email sent with embedded scatterplot images.")
    logging.info("Email sent with embedded scatterplot images.")

    # Call next process
    python_executable = sys.executable
    subprocess.run([python_executable, next_script_path])

except Exception as e:
    error_handling.handle_exception(e)