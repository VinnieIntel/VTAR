import pandas as pd
import cx_Oracle
import warnings
import os
import datetime
import logging
import error_handling

warnings.filterwarnings("ignore")

logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("===============================================")
logging.info('Process 2: Get the lot list from ARIES database')
logging.info("===============================================")

print("\n=================================================")
print(f"Process 2: Get the lot list from ARIES database")
print("=================================================")

try: 
    lot_csv_path = 'output.csv'
    df_csv = pd.read_csv(lot_csv_path)
    lot_number = df_csv['Lot Number'].iloc[0]
    tool_id = df_csv['Tool Names'].iloc[0]
    cell_id = df_csv['Cell Names'].iloc[0]

    con_aries = cx_Oracle.connect("", "", "PG.ARIES")
    sql_aries = f"""
    SELECT DISTINCT 
            v0.lot AS LOT
            ,To_Char(v0.test_start_date_time,'yyyy-mm-dd hh24:mi:ss') AS TEST_START_DATE
            ,To_Char(dt.device_end_date_time,'yyyy-mm-dd hh24:mi:ss') AS device_end_date_time
            ,v0.tester_id AS MODULE_ID
            ,dt.site_id AS CELL_ID
            ,v0.program_name AS PROGRAM_NAME
            ,v0.operation AS OPERATION
            ,ml.owner AS owner
            ,mp.prodgroup3 AS prodgroup3
            ,dt.tiu_personality_card_id AS tiu_personality_card_id
            ,dt.thermal_head_id AS thermal_head_id
            ,dt.device_tester_id AS device_tester_id
    FROM 
        A_Testing_Session v0
        LEFT JOIN A_MARS_Lot ml ON v0.lot=ml.lot -- AND ml.facility = v0.facility
        LEFT JOIN A_MARS_Product mp ON ml.product = mp.product AND ml.mars_schema=mp.mars_schema AND mp.facility = v0.facility
        INNER JOIN A_Device_Testing dt ON v0.lao_start_ww + 0 = dt.lao_start_ww AND v0.ts_id + 0 = dt.ts_id
    WHERE 1=1
        AND      v0.test_end_date_time >= SYSDATE - 200 
        AND      (v0.tester_id LIKE  '{tool_id}'
        )
        AND      (v0.lot LIKE  'M%' 
        )
        AND      dt.site_id = '{cell_id}' 
    ORDER BY
            2 ASC
    """

    df_aries = pd.read_sql(sql_aries, con_aries)

    df_aries = df_aries.drop_duplicates(subset='LOT', keep='last')
    df_aries = df_aries.reset_index(drop=True)

    the_lot_index = df_aries[df_aries['LOT'] ==lot_number].index[0]
    df_aries_the_lot = df_aries.iloc[[the_lot_index]]
    the_lot_path = "the_lot.csv"
    df_aries_the_lot.to_csv(the_lot_path, index=False)

    full_lot_list_path = 'full_lot_list.csv'
    df_aries.to_csv(full_lot_list_path, index=False)
    print(f"Output saved to '{full_lot_list_path}'") 
    logging.info(f"Output saved to '{full_lot_list_path}'") 
        

    # Generate lot_list_processed.csv
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

    # Add the new column 'LOT_SEQUENCE' with value 'N'
    df_aries_the_lot['LOT_SEQUENCE'] = 'N'

    new_column_order = ['CLS_LOT', 'LOT_SEQUENCE', 'CLS_OPERATION', 'CLS_TESTER_ID', 'CLS_THERMAL_HEAD', 
                        'DEVICE_END_DATE_TIME', 'SITE_ID', 'TIU_PERSONALITY_CARD_ID', 'DEVICE_TESTER_ID']

    df_aries_the_lot = df_aries_the_lot.rename(columns=column_mapping)
    processed_df = df_aries_the_lot[new_column_order]

    processed_df = processed_df.rename(columns=column_mapping)

    # save to csv
    processed_lot_list_path = 'lot_list_processed.csv'
    processed_df.to_csv(processed_lot_list_path, index=False)
    print(f"\n{processed_df}\n")
    logging.info(f"\n{processed_df}\n")
    print(f"Processed output saved to '{processed_lot_list_path}'")
    logging.info(f"Processed output saved to '{processed_lot_list_path}'")

    # Call next process
    next_script_path = './03_SQLPF.py'
    os.system(f'python {next_script_path}')

except Exception as e:
    error_handling.handle_exception(e)