import os
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
import mysql.connector
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

os.environ['AWS_ACCESS_KEY_ID'] = '********************'
os.environ['AWS_SECRET_ACCESS_KEY'] = '****************************'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 18, 13, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Extraction_Pipeline',
    default_args=default_args,
    description='An example DAG that runs at 1:28 PM every day',
    schedule_interval=timedelta(days=1, hours=1),
)

def start_job():
    print("Start Data Reading from AWS S3 Bucket and RDS MySQL Database...")


def read_S3_data():
    """
    Reads a CSV file from the 'client-data-source' S3 bucket with a name of the format 'bank+currentdate.csv',
    where 'currentdate' is today's date in the format 'YYYY-MM-DD', and returns a JSON string
    with the contents of the file.
    """
    bucket_name = 'client-data-source'
    s3 = boto3.client('s3')
    today = datetime.today().strftime('%Y-%m-%d')
    prefix = f'bank{today}'
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        file = response['Contents'][0]['Key']
        obj = s3.get_object(Bucket=bucket_name, Key=file)
        body = obj['Body'].read().decode('utf-8')
        data = StringIO(body)
        df = pd.read_csv(data)
        # convert DataFrame to JSON string
        json_data = df.to_json(orient='records')
        return json_data
    else:
        raise FileNotFoundError(f"No file found in {bucket_name} with prefix {prefix}")

new_json = read_S3_data()

def load_to_s3():
    """
    Reads a JSON string, converts it to a Pandas DataFrame,
    saves the DataFrame to CSV format, and uploads it to an S3 bucket.
    """

    # Convert JSON data to DataFrame
    df = pd.read_json(new_json)

    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)

    # Upload CSV data to S3 bucket
    dest_bucket = 'staging-area-datalake'
    s3 = boto3.client('s3')
    today = datetime.today().strftime('%Y-%m-%d')
    key = f'1bank_{today}.csv'
    s3.put_object(Bucket=dest_bucket, Key=key, Body=csv_data.encode('utf-8'))

    print(f"File '{key}' uploaded to S3 bucket '{dest_bucket}'.")

load_to_s3()

def read_rds():
    # Replace <hostname>, <username>, <password>, and <database_name> with your database details
    cnx = mysql.connector.connect(
        host='client-database.crukg3fyauvn.us-east-2.rds.amazonaws.com',
        user='admin',
        password='AWS$user7',
        database='client_data'
    )

    cursor = cnx.cursor()

    # Replace <table_name> with your table name
    query = "SELECT * FROM bank"
    cursor.execute(query)

    rows = cursor.fetchall()
    col_names = [i[0] for i in cursor.description]
    df = pd.DataFrame(rows, columns=col_names)

    cursor.close()
    cnx.close()

    # Convert dataframe to JSON format and save it in the df variable
    df = df.to_json(orient='records')

    return df

json_data = read_rds()

def load_s3():
    # Replace <json_string> with your JSON data
    #json_str = 'json_data'
    
    # Replace <bucket_name> with your S3 bucket name
    bucket_name = 'staging-area-datalake'

    # Convert JSON string to a pandas DataFrame
    df = pd.read_json(json_data)

    # Save DataFrame to CSV file with name containing current date
    filename = '2bank_' + datetime.now().strftime('%Y-%m-%d') + '.csv'
    csv_buffer = df.to_csv(index=False)

    # Upload CSV file to S3 bucket
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_buffer, Bucket=bucket_name, Key=filename)

load_s3()

def end_job():
    print("Data Loading to S3 Done...")

task1 = PythonOperator(
    task_id='Start_Data_Reading',
    python_callable=start_job,
    dag=dag,
)

task2 = PythonOperator(
    task_id='Read_From_S3_Bucket',
    python_callable=read_S3_data,
    dag=dag,
)

task3 = PythonOperator(
    task_id='Load_Bucket_Data_To_S3',
    python_callable=load_to_s3,
    dag=dag,
)

task4 = PythonOperator(
    task_id='Read_From_RDS_Database',
    python_callable=read_rds,
    dag=dag,
)

task5 = PythonOperator(
    task_id='Load_MySQL_Data_To_S3',
    python_callable=load_s3,
    dag=dag,
)

task6 = PythonOperator(
    task_id='Data_Loading_Done',
    python_callable=end_job,
    dag=dag,
)


task1 >> task2 >> task3
task1 >> task4 >> task5


task3 >> task6
task5 >> task6