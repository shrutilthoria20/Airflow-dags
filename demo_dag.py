from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import random
import string

default_args={"owner":"airflow",
        "retries":1,
        "retry_daily":timedelta(minutes=5),
        "start_date":datetime(2025,1,1)
    }

# Instantiate a DAG
dag = DAG(
    dag_id='demo_for_ch',
    default_args=default_args,
    description='A simple Airflow DAG with multiple tasks',
  # Set the interval at which the DAG should run
    catchup=False,
)

# Define three Python functions to be used as tasks

def verification(**kwargs):
    print("Initializing database-1 connection")
    for i in range(1,10):
        print('Connection successful...')
    print("Database connected")

verification = PythonOperator(
    task_id='verification',
    python_callable=verification,
    provide_context=True,
    dag=dag,
)

verification1 = PythonOperator(
    task_id='verification',
    python_callable=verification,
    provide_context=True,
    dag=dag,
)

def dbt_run(**kwargs):
    print("Initializing DBT connection")
    for i in range(1,10):
        print('run successful...')
    print("DBT Finished")

dbt_run = PythonOperator(
    task_id='dbt_run',
    python_callable=dbt_run,
    provide_context=True,
    dag=dag,
)

dbt_run1 = PythonOperator(
    task_id='dbt_run',
    python_callable=dbt_run,
    provide_context=True,
    dag=dag,
)

def data_replication(**kwargs):
    print("Initializing database-2 connection")
    for i in range(1,100):
        # create random print statements
        table_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        data = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
        print(f"Replicating data {data} to table {table_name}")
        print("Data inserted")
    print("Data inserted")



table_list = ['OL2W9', 'MRJ9L', 'JZYA3', 'VWN8V', 'FZIZX', 'ZWGB0', 'DNLTO', 'Z46IF', 'VFH10', '1PZ7Q', 'DZIYP', 'LVK8A', 'HLQBS', 'R6JLE', 'BMNN6', '2N9D3', 'UWXRX', '8WRNO', 'BRX6Z', 'BBRPI', 'FMALQ', 'GM5IW', 'EZ13Z', 'ROIG2', 'FIPPX', 'PCHQY', 'R12P4', 'WNPM5', '36ER5', 'D5K1U', '7S5Z9', 'R0V2V', 'MXTMO', 'VZ42X', '5I8P4', 'FURZ3', 'POK5K', '3CG3P', 'LVJB4', '909AQ', '8E36Q', 'N6AIY', 'FJNLO', '6OM15', 'JDWGS', 'PC2XE', 'JDEAM', 'XXLPA', '7P87G', '7L315', 'EADSP', '4OCTL', 'FKDXB', 'VRWDW', 'VUZUR', 'J1O38', 'VXION', 'G46LA', '95N1N', 'YLRFA', 'FVMQY', 'J5SOI', 'URNAW', 'FH49X', 'MVM5B', 'V0NS1', '4N2F7', 'CWUYU', '8GGXI', 'N898H', 'VZG7I', '8HLVL', 'XNC5R', 'F6UHL', 'PUGJT', 'E59RS', 'JIR3E', '5ASTD', '66YBA', 'XWQKN', '8FP0D', 'W2SBO', 'K8G5I', 'UPUWP', 'NBU9Z', 'N9Z6F', 'MFXIC', '0XZ90', 'D41KA', 'JU85T', 'YV6HW', '479PG', 'JX9BF', 'K1HRH', '9GOX3', '3FAES', 'WBB1Y', '7TYNL', '1XJGF', 'Y52LB', 'QCK5F', '9IA7S', 'C4FLV', 'O7KIO', 'W9FPX', '1TJWH', '4D5TT', 'EA2S3', 'H6JR7', 'W79JP', 'BAMEO', 'IQ08C', 'KXSSP', '0FGF2', '47IVO', '0AA81', 'A73Y9', 'JLVQ9', 'ESDZ4', 'T90V0', 'YVPM8', 'S17F8', 'XF7WG', 'CFD6M', 'PX811', '7D29P', 'ENUMA', 'C7H2W', 'QFXX3', '74ZA6', '642BZ', 'S3QTL', 'O1EHY', '8WPJ0', 'BLF25', 'OWKEF', '80182', 'E8DHS', 'P7THX', 'QJSJA', '04I8R', 'GZSQS', 'C58PW', 'F4YSA', '93W2A', 'YZMM5', 'TX8MR', 'GJMP0', 'T302D', 'MO9ZR', 'OZTAA', 'BM4GH', 'V1FZQ', 'DDBB5', 'HW58U', 'M2U8O', '8G3CW', 'ZEEBH', 'S6BZD', 'QXF8P', 'PT0FC', 'RHZDJ', 'L5JNN', 'EQ2JL', 'SAAN6', 'LUBZ8', 'WDWEV', 'VP87J', 'HSKE8', 'WQCI4', '5GYFZ', 'D18IZ', 'UD8V3', 'C6MBR', 'NPM00', '5DHCH', 'M3YYY', 'VNFJG', 'MRZZB', '6QIQL', 'T33L2', 'YB3ZY', 'DB0A6', '5QB9F', '0W4LK', 'ACQGC', 'SEKO7', 'VALZ0', '5EF6G', 'XZF3Y', '1OJXH', 'Q305F', '7YDVI', 'H05EZ', 'VEIRE', 'Q61MI', 'ZV650', 'NV6JH', 'MAWPQ', 'UPPJB', 'DXOOI', 'VBB5I', 'E656Y', 'SZL83', 'P8ORC', '5QOC9', '83AQD', 'KD8JG', '69JVK', '0PF3D', '4RBH7', 'WX05R', '4FLBH', 'YDEZ6', 'J80E2', 'SSPE2', 'R92C4', 'W575F', 'UW6W4', 'UIIAW', 'SG4HT', '78E0L', 'P04QS', 'KY6X4', '3WTYS', 'WVJNU', '0KVZK', 'TARNV', 'YBVUV', 'M6DYY', 'RVH8W', 'I3KIT', 'RQMLG', '6D7SU', '58CAL', 'B2US9', 'MZ8XZ', '4DKTP', 'HP7F6', 'EWAHQ', 'K1YXT', 'HPEGC', 'M9ZYS', 'DPKC5', '9JZEP', 'HNWKK', 'MN7F9', 'DVAYJ', 'POHY0', '8UFQ8', 'JUB26', '3RZFK', '2E22F', 'RDQ1D', 'PPHVC', 'SN8A5', '8NVRF', '5GZ8L', 'WLVML', 'FIS8J', '1C3Q9', 'JBQEO', 'TDZEP', 'K8KST', 'C447U', 'H88SM', 'O3WL3', 'VGJL5', 'MN3ZH', 'A5ZQK', 'KKM0O', '3FDZT', 'E8T62', 'TZNDH', 'DLQ0C', 'AO045', 'RH2TW', 'HM9QE', 'L5OVT', 'B350X', 'GWBJ7', 'FRUX9', 'EY1DD', 'YC4HS', 'DR7KX', 'WXSMP', 'PYXJO', 'KHL20', 'RKI8T', 'OHR2L', 'LKEH2', '5TWQH', 'FRMG0', 'SE4MQ', 'EN273', 'OLOCS', 'UTAUA', 'TFCZ4', '3I6LN', 'W2690', 'RHHIK', '0USZU', 'BWIAJ', 'QZKUR', '1F087', '0NQ0W', 'L4DV3', 'YJ1K8', 'GREA7', 'N1HU2', '8TK8E', 'VPIMQ', '3SD21', 'S730G', '261X8', 'Y2XQ8', 'P9SIO', 'INOL5', '38RED', '19NS8', '0OW8P', 'GSKIT', 'CK6LG', 'NN2KJ', 'FR8EP', '837WS', 'AL7MB', 'PM0RK', '26T2W', '078T2', 'YKFG8', 'GKRSL', '1K9K5', '25QMZ', 'X6UG8', '84XLQ', 'T0YDE', '8EM5G', 'LA6ZX', 'UCHF7', 'VYJMD', 'LQ0M2', '07WGI', '5TNDT', 'BORE9', 'ZSK6X', '2G1BS', 'X3HNG', 'W5L6C', 'A78E6', 'MLNZ2', 'LK85U', 'G7O65', 'DCGSU', 'UK88Y', 'HECDI', 'X02Y6', 'Q7GZ5', 'GK8QU', 'M7MTN', 'RKM5P', 'MHK4K', 'OAZNA', '0VTEP', 'T1Z7D', 'V7S71', 'BY5VN', 'OUDF7', 'PT0VW', '0BUSD', '6E8BZ', '7Z9DT', 'MIMNB', 'IRX47', 'LVGRK', 'XYMWR', 'PQV9H', 'NU5SM', 'SIQJQ', 'NGL47', '8RFGY', 'FHM05', 'IP19O', 'HQR4B', 'TGHDM', '3E6NY', 'G5HCE', 'BUOXL', 'U1NZO', '6G72K', 'VWMF1', 'T35QU', 'V7N4E', 'QBTXQ', 'KWZVQ', 'C95QS', 'II745', 'XZAU3', '6ZOPD', 'UOM3I', 'QV790', 'L4HIA', '8JNCW', 'E5Q21', 'GG31Y', 'RHCO8', 'ND128', '110NI', 'KFB14', '8RIQ5', '6VQKM', 'ENNW1', 'EGX1X', 'A1UBQ', 'O5X7W', '1I1SZ', 'S7DXN', '13Q00', 'WTJH4', 'K9Y5L', 'BVXSB', 'K0CGQ', 'DY9BO', 'IKU8B', 'C4LDZ', '7OWM2', '741N4', 'WZWE4', '1P27G', '5HK9B', '81WOG', 'A3V17', 'I7LJS', 'CE5X4', 'YO0P3', 'WQZ7R', 'U7X6N', 'BEDO5', 'Q3K39', 'T0YB2', 'FWNSI', 'TMDM3', 'JZXT3', '7R1KL', '3I6YK', 'QMJ9B', 'EJZZZ', 'ZL4B6', '3EGA0', 'ZNO9F', 'T72EX', 'AFNXF', 'G44WE', '7BGBO', '6ZJAW','RZWSV', 'JEQB7', 'SWWJE', 'Z7ZC7', 'GZA58', '6YIM2', '07SSF', 'NCW8R', 'RR83O', 'SKW0X', 'JTPIC', '4DMHU', 'K5JJ2', '34SIX', 'F9XDU', 'NN4Z2', 'O4E57', 'D1OOI', 'RS2DD', 'CFWNQ', 'OTTFU', 'DHD1O', 'SWZAY', 'TRR5U', 'ZMJ04', 'JS178', 'L8CSF', '3V5UO', '01CAO', 'YZZTY', 'LIV0X', 'RMVE7', '0BGIC', 'FLQLX', 'C12IM', 'ECYNU', '1VN0Y', '8A23R', 'HB4E5', 'GTFAJ', '2OTDY', '79EZS', 'UFTOB', 'TY7L2', 'EKS8W']

table_list2=["table1","table2","table3","table4","table5","table6"]
for table in table_list2:
    task1 = PythonOperator(
        task_id=table,
        python_callable=data_replication,
        provide_context=True,
        dag=dag,
    )
    verification1 >> [task1]
    [task1] >> dbt_run1

for table in table_list:
    task = PythonOperator(
        task_id=table,
        python_callable=data_replication,
        provide_context=True,
        dag=dag,
    )
    verification >> [task]
    [task] >> dbt_run