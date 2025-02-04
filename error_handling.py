import logging
import traceback
import win32com.client as client
import os

def send_error_email(error_message):
    try:
        with open('email_entry_id.txt', 'r') as file:
            email_entry_id = file.read().strip()
        outlook = client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        mail_item = outlook.GetItemFromID(email_entry_id)

        reply = mail_item.ReplyAll()
        reply.Subject = "Error in Script Execution"
        # reply.To = "pgat.test.maps@intel.com" # Use this when deploy
        reply.To = "soon.thiam.ong@intel.com"

        script_dir = os.path.dirname(__file__)
        log_file_path = os.path.join(script_dir, 'log_file.log')

        # Create the HTML body content
        html_body = f"""
        <html>
        <body>
            <p><b>An error occurred during the execution of the script:</b></p>
            <p>{error_message}<br><br></p>
            <p>Please check the log file for more details to identify and fix the error in the script.</p>
            <p><a href="file:///{log_file_path}">{log_file_path}</a><br></p>
            <p>**MEOS team, please proceed with manually plotting the scatterplots and boxplots. Thank you! :)</p>
        </body>
        </html>
        """

        # Set the HTML body of the reply
        reply.HTMLBody = html_body + reply.HTMLBody
        reply.Save()
        reply.Send()
    except Exception as e:
        logging.error(f"Failed to send error email: {e}")
        logging.error(traceback.format_exc())

def handle_exception(e):
    error_message = f"An unexpected error occurred: {e}"
    print(error_message)
    logging.error(error_message)
    logging.error(traceback.format_exc())
    send_error_email(error_message)