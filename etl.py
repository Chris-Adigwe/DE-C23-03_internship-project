import pandas as pd
import numpy as np
import requests, os, dotenv, io, boto3, glob
from datetime import datetime, timedelta
from io import StringIO
from datetime import datetime
from dotenv import dotenv_values
from botocore import UNSIGNED
from botocore.client import Config
from utils import get_dw_conn, search_filenames, make_keys_dataframe, last_updated, create_dt_from_file_path, check_table_exists, move_files, create_table

dotenv_values()

# Create a boto3 s3 client for bucket operations
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
s3_resource = boto3.resource('s3')

bucket_name = "tenalytics-internship-bucket"
path = 'orders_data'


def extract_files_from_s3():

    objects_list = s3.list_objects(Bucket=bucket_name, Prefix=path)
    files = objects_list.get('Contents')
    keys = [file.get('Key') for file in files][1:]
    objs = [s3.get_object(Bucket=bucket_name, Key=key) for key in keys]

    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"

    order_df = make_keys_dataframe('orders_data/orders', keys)
    order_df.to_csv(f"data/raw/order/order_{file_name}.csv", index=False)
    print("orders.csv has been pushed to staging area")
    reviews_df = make_keys_dataframe('orders_data/reviews', keys)
    reviews_df.to_csv(f"data/raw/reviews/reviews_{file_name}.csv", index=False)
    print("reviews.csv has been pushed to staging area")
    shipment_df = make_keys_dataframe('orders_data/shipment_deliveries', keys)
    shipment_df.to_csv(
        f"data/raw/shipments/shipment_{file_name}.csv", index=False)
    print("shipments.csv has been pushed to staging area")

    print("All data has been pushed to raw folder")


def transform_data():
    con = get_dw_conn()
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    order_last_date = last_updated(''' SELECT MAX(order_date) FROM chriadig5596_staging.orders''', con)
    shipment_last_date = last_updated(''' SELECT MAX(shipment_date) FROM chriadig5596_staging.shipments_deliveries''', con)
    
    #create dataframe from files downloaded for orders, shipments and reviews 
    order_df = pd.read_csv(glob.glob('data/raw/order/*.csv')[0])
    shipment_df = pd.read_csv(glob.glob('data/raw/shipments/*.csv')[0])
    review_df = pd.read_csv(glob.glob('data/raw/reviews/*.csv')[0])
    
    #check if there is a last date in order date
    if order_last_date == None:
        order_df['order_date'] = pd.to_datetime(order_df['order_date']).dt.date
        order_df['date_ingested'] = datetime.now().date()
        order_df.to_csv(
            f"data/transformed/orders/order_transformed_{file_name}.csv", index=False)

    else:
        order_df['order_date'] = pd.to_datetime(order_df['order_date']).dt.date
        order_df['date_ingested'] = datetime.now().date()
        order_df = order_df[order_df['order_date'] > pd.to_datetime(order_last_date).date()]
        order_df.to_csv(
            f"data/transformed/orders/order_transformed_{file_name}.csv", index=False)

    #move order raw files to archive 
    move_files('data/raw/order/', 'data/archived/raw/orders/')



    #check if there is a last date in order date
    if shipment_last_date == None:
        shipment_df['shipment_date'] = pd.to_datetime(shipment_df['shipment_date']).dt.date
        shipment_df['delivery_date'] = pd.to_datetime(shipment_df['delivery_date']).dt.date
        shipment_df['date_ingested'] = datetime.now().date()
        #shipment_df = shipment_df.replace({np.NaN: None})

        shipment_df.to_csv(
            f"data/transformed/shipments/shipment_transformed_{file_name}.csv", index=False)

    else:
        shipment_df['shipment_date'] = pd.to_datetime(shipment_df['shipment_date']).dt.date
        shipment_df['delivery_date'] = pd.to_datetime(shipment_df['delivery_date']).dt.date
        shipment_df['date_ingested'] = datetime.now().date()
        #shipment_df = shipment_df.replace({np.NaN: None})
        shipment_df = shipment_df[shipment_df['shipment_date']
                                  > pd.to_datetime(shipment_last_date).date()]
        shipment_df.to_csv(
            f"data/transformed/shipments/shipment_transformed_{file_name}.csv", index=False)
        
    #move shipment raw files to archive 
    move_files('data/raw/shipments/', 'data/archived/raw/shipments/')

    
    review_df['data_ingested'] = datetime.now().date()
    review_df.to_csv(
        f"data/transformed/reviews/review_transformed_{file_name}.csv", index=False)
    
    #move review raw files to archive 
    move_files('data/raw/reviews/', 'data/archived/raw/reviews/')

    print('transformation complete')


def load_data():
    con = get_dw_conn()
    cur = con.cursor()
    order_file = glob.glob('data/transformed/orders/*.csv')[0]
    review_file = glob.glob('data/transformed/reviews/*.csv')[0]
    shipment_file = glob.glob('data/transformed/shipments/*.csv')[0]

    

    #for the order table
    print('data loading for order table')
    with open(order_file, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'chriadig5596_staging.orders', sep=',')

    move_files('data/transformed/orders/', 'data/archived/transformed/orders/')
    print('data successfully loaded to order table')

    # for the review table
    print('data loading for review table')
    with open(review_file, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'chriadig5596_staging.reviews', sep=',')

    move_files('data/transformed/reviews/', 'data/archived/transformed/reviews/')
    print('data successfully loaded to review table')



    # for the shipment table
    cur.execute('''
                        CREATE TEMPORARY TABLE shipments_deliveries(
                            shipment_id    INT NOT NULL,
                            order_id  INT NOT NULL,
                            shipment_date TEXT,
                            delivery_date TEXT,
                            date_ingested date)
        ''')
        
    with open(shipment_file, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'shipments_deliveries', sep=',')

    cur.execute('''
    INSERT INTO chriadig5596_staging.shipments_deliveries (shipment_id, order_id, shipment_date, delivery_date, date_ingested)
    SELECT 
            shipment_id, 
            order_id, 
            CASE
               WHEN shipment_date = '' THEN NULL
               ELSE TO_DATE(shipment_date, 'YYYY-MM-DD')
           END,
            CASE
               WHEN delivery_date = '' THEN NULL
               ELSE TO_DATE(delivery_date, 'YYYY-MM-DD')
           END, date_ingested
        
            FROM shipments_deliveries
        ''')
      

    move_files('data/transformed/shipments/', 'data/archived/transformed/shipments/')
    
    con.commit()

    cur.close()
    con.close()


    print('data successfully loaded to shipment table')



        






extract_files_from_s3()

create_table()

transform_data()

load_data()

