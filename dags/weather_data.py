from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum
from os.path import join
import pandas as pd

with DAG(
        'weather_data',
        start_date=pendulum.datetime(2023, 1, 4, tz="UTC"),
        schedule_interval='0 0 * * 1', # monday
) as dag:

    task_one = BashOperator(
        task_id = 'create_new_folder',
        bash_command='mkdir -p "/home/gustavo/Documents/airflowpipeline/week={{ data_interval_end.strftime("%Y-%m-%d") }}"' #mkdir -p just because if this folder already exists, nothing gonna happen - bash_command for write the command you'd like to execute 
    )

    def get_raw_data(data_interval_end):
        
        city = 'Boston'
        key = 'BGNJPFH697FZ6J229UCTBM3AH'

        URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
                f"{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv")

        get_data_from_api = pd.read_csv(URL)

        file_path = f'/home/gustavo/Documents/airflowpipeline/week={data_interval_end}/'        

        get_data_from_api.to_csv(file_path + 'raw_data.csv')
        get_data_from_api[['datetime','tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperature.csv')
        get_data_from_api[['datetime', 'description', 'icon']].to_csv(file_path + 'conditions.csv')

    task_two = PythonOperator(
        task_id = 'get_raw_data',
        python_callable = get_raw_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    task_one >> task_two