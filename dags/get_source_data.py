# Airflow libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# Dependency libraries
import requests
import pandas as pd
import pandasql as ps
import numpy as np
import sqlite3 as db
from bs4 import BeautifulSoup
import sys
from shutil import copyfile


src = '/Users/anegron/projects/airflow_demo/demo.sqlite'
dst = '/Users/anegron/projects/rr_demo/demo.sqlite'

default_args = {
    'owner': 'Alberto Negron',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 4),
    'catchup': False,
    'email': ['alberto@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('source_data', description='Demo for RR interview', default_args=default_args,
          schedule_interval='0 12 * * *')


def db_conn():
    return db.connect(src)


def db_close_conn():
    db_conn().close()


def db_copy():
    copyfile(src, dst)
    return "DB has been copied to destination"


def copy_plots():
    copyfile('/Users/anegron/projects/airflow_demo/airlines_by_country.png',
             '/Users/anegron/projects/rr_demo/airlines_by_country.png')
    copyfile('/Users/anegron/projects/airflow_demo/airports_by_country.png',
             '/Users/anegron/projects/rr_demo/airports_by_country.png')


def scrapper(lista):
    container = []
    for item in lista:
        page = requests.get(item)
        soup = BeautifulSoup(page.text, 'html.parser')
        product_list = soup.find(class_='products products-list')
        product_title = [item.text for item in product_list.find_all(
            'a', attrs={'class': 'product-title'})]
        product_descr = [item.text for item in product_list.find_all(
            'div', attrs={'class': 'descr'})]
        product_price = [item.find(class_='currency').text for item in product_list.find_all(
            'span', attrs={'class': 'price-value'})]
        products = list(zip(product_title, product_descr, product_price))
        container.append(products)
    return container


def run_scrapper():
    conn = db_conn()
    links = ['https://www.stationaryengineparts.com/Lister-Start-O-Matic-Spares/',
             'https://www.stationaryengineparts.com/Lister-A-and-B-spares/',
             'https://www.stationaryengineparts.com/Lister-HA-HB-HL-HR-and-HW-Engine-Spares/']
    scrapped_data = scrapper(links)
    flatten_data = [l2 for sublist in scrapped_data for l2 in sublist]
    df = pd.DataFrame(flatten_data, columns=[
                      'product', 'description', 'price'])
    df['quantity'] = np.random.randint(1, 2000, df.shape[0])
    df.to_sql('products', conn, if_exists='replace')


def read_airlines():
    conn = db_conn()
    raw_data = '/Users/anegron/projects/airflow_demo/data/airlines.dat'
    df = pd.read_csv(raw_data, names=[
                     'id', 'name', 'alias', 'iata', 'icao', 'callsign', 'country', 'active'], index_col=False)
    q = "Select country, active, count(*) as n from df group by 1,2 "
    df1 = ps.sqldf(q)
    df.to_sql('airlines', conn, if_exists='replace')
    df1.to_sql('air_stats', conn, if_exists='replace')


# Run Scrapper
t1 = PythonOperator(task_id='run_scrapper',
                    python_callable=run_scrapper, retries=0, dag=dag)

load_command = "jupyter nbconvert --to notebook --execute /Users/anegron/projects/airflow_demo/scripts/load_flight_info.ipynb"

t2 = BashOperator(task_id='load_flight_data',
                  bash_command=load_command,
                  dag=dag)

trns_command = "jupyter nbconvert --to notebook --execute /Users/anegron/projects/airflow_demo/scripts/transform_flight_data.ipynb"

t3 = BashOperator(task_id='transform_flight_data',
                  bash_command=trns_command,
                  dag=dag)

t4 = PythonOperator(task_id='copy_plots',
                    python_callable=copy_plots, retries=0, dag=dag)

t5 = PythonOperator(task_id='db_close',
                    python_callable=db_close_conn, retries=0, dag=dag)

t6 = PythonOperator(task_id='db_copy',
                    python_callable=db_copy, retries=0, dag=dag)

t1
t2
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t5.set_upstream(t1)
t6.set_upstream(t5)
