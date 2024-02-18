from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import logging
from airflow.utils.task_group import TaskGroup
import boto3

def fetch_latest_data_task(**kwargs):
    url = "https://api.openaq.org/v2/latest"
    all_data = []
    extracted_data = []
    # Start with the first page
    page = 1

    while True:
        # Make a request to the API
        params = {
            'limit': 100,
            'page': page,
            'offset': (page - 1) * 100,
            'sort': 'desc',
            'radius': 1000,
            'order_by': 'lastUpdated',
            'dump_raw': False
        }
        response = requests.get(url, params=params)
        data = response.json()

        if 'results' not in data or not data['results']:
            break
        

        for result in data['results']:
            location = result['location']
            city = result['city']
            country = result['country']
            latitude = result['coordinates']['latitude']
            longitude = result['coordinates']['longitude']
            measurements = result['measurements']
            
            for measurement in measurements:
                parameter = measurement['parameter']
                value = measurement['value']
                last_updated = measurement['lastUpdated']
                unit = measurement['unit']
                
                # Append the extracted data to the list
                extracted_data.append([location, city, country, latitude, longitude, parameter, value, last_updated, unit])
        page = page+1
    # Create a DataFrame from the extracted data
    df = pd.DataFrame(extracted_data, columns=['location', 'city', 'country_cd', 'latitude', 'longitude', 'parameter', 'value', 'last_updated', 'unit'])

    # Define the CSV file path
    csv_file_path = "air_quality_data.csv"

    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)

    s3_client = boto3.client('s3')
    # Upload the CSV file to S3
    s3_bucket_name = 'aqi-data-bkt'
    s3_folder = 'daily_data/'
    s3_key = f'{s3_folder}air_quality_data.csv'
    s3_client.upload_file(csv_file_path, s3_bucket_name, s3_key)

    logging.info(f"CSV file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_key}")


def fetch_cities_data(**kwargs):
    url = f"https://api.openaq.org/v2/cities"
    extracted_data = []
    page = 1

    while True:
        params = {
            'limit': 100,
            'page': page,
            'offset': (page - 1) * 100,
            'sort': 'asc',
            'order_by': 'city'
        }
        response = requests.get(url, params=params)
        data = response.json()
          
            
        if 'results' not in data or not data['results']:
            break

        for result in data['results']:
            delimiter = '|'
            country = result['country']
            city = result['city']
            count = result['count']
            first_updated = result['firstUpdated']
            last_updated = result['lastUpdated']
            parameters =  delimiter.join(data['results'][0]['parameters'])

            extracted_data.append([country,city,count,first_updated,last_updated,parameters])
            
        page = page+1
    df = pd.DataFrame(extracted_data,columns =['country','city','count','first_updated','last_updated','parameters'])

    csv_file_path='cities_data.csv'
    df.to_csv(csv_file_path,index=False)

    s3_client = boto3.client('s3')
    # Upload the CSV file to S3
    s3_bucket_name = 'aqi-data-bkt'
    s3_folder ='cities/'
    s3_key = f'{s3_folder}cities_data.csv'
    s3_client.upload_file(csv_file_path, s3_bucket_name, s3_key)

    logging.info(f"CSV file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_key}")



def fetch_countries_data(**kwargs):
    url = f"https://api.openaq.org/v2/countries"
    extracted_data = []
    page = 1

    while True:
            # Make a request to the API
        
        params = {
            'limit': 100,
            'page': page,
            'offset': (page - 1) * 100,
            'sort': 'asc',
            'order_by': 'name'
        }
                

        response = requests.get(url, params=params)
        data = response.json()
          
            
        if 'results' not in data or not data['results']:
            break

        for result in data['results']:
            delimiter = '|'
            code = result['code']
            name = result['name']
            locations = result['locations']
            first_updated = result['firstUpdated']
            last_updated = result['lastUpdated']
            parameters =  delimiter.join(data['results'][0]['parameters'])
            count = result['count']
            cities = result['cities']
            sources = result['sources']

            extracted_data.append([code,name,locations,first_updated,last_updated,parameters,count,cities,sources])
            
        page = page+1
    df = pd.DataFrame(extracted_data,columns =['code','name','locations','first_updated','last_updated','parameters','count','cities','sources'])

    csv_file_path='countries_data.csv'
    df.to_csv(csv_file_path,index=False)

    s3_client = boto3.client('s3')
        # Upload the CSV file to S3
    s3_bucket_name = 'aqi-data-bkt'
    s3_folder = 'countries/'
    s3_key = f'{s3_folder}countries_data.csv'
    s3_client.upload_file(csv_file_path, s3_bucket_name, s3_key)

    logging.info(f"CSV file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_key}")


def fetch_parameters_data(**kwargs):
    url = f"https://api.openaq.org/v2/parameters"
    extracted_data = []
    page = 1

    while True:
            # Make a request to the API
        
        params = {
                    'limit': 100,
                    'page': page,
                    'offset': (page - 1) * 100,
                    'sort': 'asc',
                    'order_by': 'name'
        }
                

        response = requests.get(url, params=params)
        data = response.json()
          
            
        if 'results' not in data or not data['results']:
            break

        for result in data['results']:
                id = result['id']
                name = result['name']
                display_name = result['displayName']
                description = result['description']
                preferred_Unit = result['preferredUnit']
                
                extracted_data.append([id,name,display_name,description,preferred_Unit])
            
        page = page+1

    df = pd.DataFrame(extracted_data,columns =['parameter_id','name','display_name','description','preferred_Unit'])

    csv_file_path='parameters_data.csv'
    df.to_csv(csv_file_path,index=False)

    s3_client = boto3.client('s3')
        # Upload the CSV file to S3
    s3_bucket_name = 'aqi-data-bkt'
    s3_folder = 'parameters/'
    s3_key = f'{s3_folder}parameters_data.csv'
    s3_client.upload_file(csv_file_path, s3_bucket_name, s3_key)

    logging.info(f"CSV file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_key}")
        




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG('AQI_DAG',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = DummyOperator(
            task_id = 'tsk_start'  
        )
        with TaskGroup(group_id='group_a') as group_A:
        
            fetch_latest_data_task = PythonOperator(
            task_id='tsk_fetch_latest_data_task',
                python_callable=fetch_latest_data_task,
                provide_context=True
            )

            fetch_cities_data_task = PythonOperator(
                task_id = 'tsk_fetch_cities_data',
                python_callable = fetch_cities_data
            )

            fetch_counties_data_task = PythonOperator(
                task_id = 'tsk_fetch_countries_data',
                python_callable = fetch_countries_data
            )

            fetch_parameters_data_task = PythonOperator(
                task_id = 'tsk_fetch_parameters_data',
                python_callable = fetch_parameters_data
            )

            fetch_latest_data_task
            fetch_cities_data_task >> fetch_counties_data_task >> fetch_parameters_data_task
    
        start_pipeline >> group_A
