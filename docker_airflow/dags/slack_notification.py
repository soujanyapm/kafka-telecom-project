from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from data_cleaning import split_df
import pandas as pd
import os

import requests
import json

from kafka import KafkaConsumer

mydir=os.path.join(os.getcwd(),"dags/")
def push_notification():
    consumer = KafkaConsumer('CALL_DATASETS',auto_offset_reset ='earliest')
    for message in consumer:
        #print ((message.key).decode("utf-8") )
        call_data = json.loads((message.value).decode('utf-8')) # decode converts byte to string, json.loads converts stringified dictionary to dictionary

        #print(json.loads((message.value).decode("utf-8")))
        if (call_data['CALL_DIRECTION'] == 'Incoming'):
            URL = "<webhook url>" #slack webhook url
            body = {"text":"Hello ! It's an incoming call "}
            headers = {"Content-Type":"application/json"}

            result = requests.post(URL, data = json.dumps(body),headers=headers) #json.dumps converts python object to json object

            print(result)
   

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=120)
}

dag=DAG('push_notification',
         start_date=datetime(2022, 1, 4),
         max_active_runs=2,
         schedule_interval= "@daily",
         default_args=default_args,
         catchup=False
         ) 


start_dummy = DummyOperator(
    task_id='start',
    dag=dag,
    )

generate_data = PythonOperator(
  task_id='generate_data',
  python_callable=push_notification, #Registered method
  provide_context=True,
  dag=dag
)


end_dummy = DummyOperator(
    task_id='end',
    dag=dag,
    )

start_dummy >> generate_data >> end_dummy


