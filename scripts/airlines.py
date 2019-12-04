import pandas as pd
import pandasql as ps


def read_airlines():
    raw_data = '/Users/anegron/projects/airflow_demo/data/airlines.dat'
    df = pd.read_csv(raw_data, names=[
                     'id', 'name', 'alias', 'iata', 'icao', 'callsign', 'country', 'active'], index_col=False)
    q = "Select country, active, count(*) as n from df group by 1,2 "
    df1 = ps.sqldf(q)
