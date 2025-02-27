1. Set up Oracle by following the link below  
https://engage.cloud.microsoft/main/threads/eyJfdHlwZSI6IlRocmVhZCIsImlkIjoiOTUxMDc0OTYyNjYxMzc2In0?domainRedirect=true

** if got multiple oracle in system, add the one you want to use at user variables manually. (eg.C:\Oracle\product\11.2.0\client_k64\bin)


2. Download Visual Studio Tools with c++ devleopment tools
https://visualstudio.microsoft.com/downloads/

else will cause error when installing and importing cx_oracle due to missing dependencies


3. Reference for SQLPF Python API
https://wiki.ith.intel.com/display/SQLPathFinder/Using+SQLPathFinder+Standard+Python+API+Client+%3A+SQLPFSvcClient.py


4. Reference for Folders set up at atdfile3
https://content.sp2019.intel.com/sites/ATMICMPCS/RFCvault/45_ICMPCS_PBICVmin_CSR_HDMX/HDMX%20Vmin%20shift%20monitor-offline%20script%20usage%20instruction_for%20PDF%20usage.pdf


5. Create a new python virtual environment if the current one is not working. Then go into the .venv and download the requirements needed using 
```
python -m venv myenv
myenv\Scripts\activate
pip install -r requirements.txt
```
If meet > ERROR: Could not find a version that satisfies the requirement, 
try
```
pip install -r requirements.txt--index-url https://pypi.org/simple
```
6. To rerun the script when there is an interupt, perhaps due to error occur, make sure
    - open atdfile3 HIST folder, clear it.
    - run from the last script, 10_movefiles.py (to make sure VminFilesPlot folder is deleted) this will then call first script and so on.
    - delete the email_entry_id file content if wanted to run back the same latest vmin triggering email.




==============================
  PYTHON SCRIPT EXPLANATION  
==============================

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Process 1: Retrieve information from email

- Delete previous files used for previous trigger email 
- Check the mailbox to look for new vmin downshift trigger email
- Write the information of lot from email into {output.csv}
- Update the {email_entry_id.txt}
- Check the mailbox again after xx duration for new trigger email if there is no new email found


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Process 2: Get the lot list from ARIES database

- Get lot_number from {output.csv}
- Pass the lot_number to cx_Oracle to retrieve lot list 
- Update {full_lot_list.csv} with data retrieved from ARIES database
- Update {the_lot.csv} with N
- Update {lot_list_processed.csv} with N


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Process 3: SQLPathFinder Python API to Get 3 Files

- Prepare the Required files into atdfile3 environment
- Get the_lot 
- Pass the_lot into SQLPF Python API
- If error when retrieving the 3 vmin files, the files in HIST folder will be checked and files with the current processing 'the_lot' will be deleted. Retry after 5mins.


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 4: Get the limit for the TP

- Get the x_iqr_value and delta_value from find_limit.csv


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 5: Check the Transaction Status of the lot

- Get the status by retrieving it from database (In progress or already move out from the tool)


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 6: Plot the Scatter Plot with limit line

- Use files start with {vmin_result_*} to plot scatterplot
- OPEN and read the file's "fac_lot" to get the lot_number
- Scatterplot:
    - RED => filtered_df_with_hw  (hw_type == unit_tester_id)
    - BLUE => filtered_df_other_hw
- RENAME the vmin_result file to include the lot number
- Move the vmin_result file processed to storage 
- Update {lot_list_processed.csv} with "clean/downshift" for STATUS
- Current lot in the past sequence (N-) downshift -> 02A_thelot_before.py 
- Current lot in the past sequence (N-) clean -> 02B_thelot_after.py
- Current lot in the future sequence (N+) downshift -> 02A_thelot_before.py 
- Current lot in the future sequence (N+) clean -> 06_workingpath.py 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 7: Move Vmin Files to Local Environment

- RENAME vmin files start with {vmin_2*} to include lot number
- Move them from atdfile HIST folder to local 'VminFilesPlot' folder to plot boxplots


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 8: Plot the boxplot by Tool Cells

- Get vmin values to plot from {output.csv}
- Do mapping so boxplots are in sequence
- For each vmin values (Vmin@...@...@...), plot the combined boxplots


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 9: Send Respond Email Back


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Process 10: Move Processed Files to Storage

- RENAME {rawdata_*} files to include lot number
- Move all the files from atdfile3 HIST -> atdfile3 "Triggering lot" folder
- Clear VminFilesPlot folder


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
** Make sure to have your Outlook closed on desktop when running the script 