import psycopg2
from sqlalchemy import create_engine
import dotenv, os
from dotenv import dotenv_values
import pandas as pd
import shutil
import psycopg2, boto3
import re
import io
from io import StringIO
from botocore import UNSIGNED
from botocore.client import Config
import glob

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
s3_resource = boto3.resource('s3')

bucket_name = "tenalytics-internship-bucket"
path = 'orders_data'

def get_dw_conn():
    dotenv.load_dotenv(r"C:\Users\USER\Desktop\projects\.env")
    DW_USERNAME = os.getenv('DW_USERNAME')
    DW_PASSWORD = os.getenv('DW_PASSWORD')
    DW_ID = os.getenv('DW_ID')
    DW_HOST = os.getenv('DW_HOST')
    DW_PORT = os.getenv('DW_PORT')
    DW_DB_NAME = os.getenv('DW_DB_NAME')
    DW_STAGING_SCHEMA = os.getenv('DW_STAGING_SCHEMA')

    conn = psycopg2.connect(f'dbname={DW_DB_NAME} user={DW_USERNAME} password={DW_PASSWORD} host={DW_HOST} port={DW_PORT}')

    return conn




def move_files(source_folder, destination_folder):
    files = os.listdir(source_folder)
    for file in files:
        source_path = os.path.join(source_folder, file)
        destination_path = os.path.join(destination_folder, file)
        shutil.move(source_path, destination_path)
        print(f"Moved file: {file}")


def search_filenames(pattern, filenames):
    matched_filenames = []
    
    for filename in filenames:
        if re.search(pattern, filename):
            matched_filenames.append(filename)
    
    return matched_filenames

def make_keys_dataframe(args, keys):
    keys = search_filenames(args, keys) 
    objs = [s3.get_object(Bucket = bucket_name, Key = key) for key in keys]
    dfs = [pd.read_csv(io.BytesIO(obj['Body'].read())) for obj in objs]
    data = pd.concat(dfs)

    return data
    
def last_updated(query,con):
    conn = get_dw_conn()
    last_updated = pd.read_sql(query, con=con).values.tolist()[0][0]
    return last_updated

def create_dt_from_file_path(file_path):
    files = glob.glob(file_path + '/*.csv')
    data = [*map(lambda filename: pd.read_csv(filename), files)]
    df = pd.concat(data, axis=0, ignore_index=True)
    return df


def check_table_exists(table_name):
    conn = get_dw_conn()
    cursor = conn.cursor()

    # Query to check if the table exists
    query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"

    cursor.execute(query)
    exists = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return exists


def create_table():
        con = get_dw_conn()
        cur = con.cursor()

        #create order table
        cur.execute('''
                        CREATE TABLE IF NOT EXISTS chriadig5596_staging.orders(
                            order_id    INT NOT NULL,
                            customer_id INT NOT NULL,
                            order_date  DATE NOT NULL,
                            product_id  INT NOT NULL,
                            unit_price  INT NOT NULL,
                            quantity    INT NOT NULL,
                            total_price INT NOT NULL,
                            date_ingested date NOT NULL)
        ''')
        print('order table created')

        #create review table
        cur.execute('''
                        CREATE TABLE IF NOT EXISTS chriadig5596_staging.reviews(
                            review    INT NOT NULL,
                            product_id  INT NOT NULL,
                            date_ingested date NOT NULL)
        ''')
        print('review table created')


        #create shipment table
        cur.execute('''
                        CREATE TABLE IF NOT EXISTS chriadig5596_staging.shipments_deliveries(
                            shipment_id    INT NOT NULL,
                            order_id  INT NOT NULL,
                            shipment_date date,
                            delivery_date date,
                            date_ingested date)
        ''')

        print('shipment table created')

        con.commit()
        cur.close()
        con.close()



