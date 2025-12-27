# import pandas as pd
import io
import argparse
import random
import os
import requests
from google.cloud import storage
import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_authentication.json"
time.sleep(30)
def main(args):
    bucket = args.bucket

    storage_client = storage.Client()
    items = storage_client.list_blobs(bucket)
    items = [blob.name for blob in items if blob.name != 'raw_data/']
    print(items)
    gcp_bucket = storage_client.bucket(bucket)
    

    # car_type = ['yellow']
    # months = ['01','02']
    years = ['2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025']
    car_type = ['fhv','yellow','green']
    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    # years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025']
    for i in car_type:
        for j in years:
            for k in months:
                # print(j+'-'+k)
                file_name = f'{i}_tripdata_{j+'-'+k}.parquet'
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/'+file_name
            

                if 'raw_data/'+file_name in items:
                    print(f'file already here: '+file_name)
                    continue 
                else:
                    print('uploading file: '+file_name)
                    target = f"raw_data/{file_name}"

                    new_item = gcp_bucket.blob(target)

                    try:
                        response = requests.get(url, stream=True)
                        with requests.get(url, stream=True) as response:
                            response.raise_for_status()
                            new_item.upload_from_file(response.raw,timeout=720, content_type='application/octet-stream')
                            print("upload complete: "+file_name)
                            time.sleep(random.uniform(65.0, 95.0))
                    except:
                        print(response.status_code)
                        time.sleep(random.uniform(15.0, 25.0))
                        # print('this file does not exist: '+url)


    # file types will be yellow, green, fhv, fhvhv
    # url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{month}.parquet'
    # print(url)
    # response = requests.get(url)
    # print(dir(response))
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to PostgreSQL')

    parser.add_argument('--bucket', help='bucket', required = True)

    args = parser.parse_args()
    main(args)
    