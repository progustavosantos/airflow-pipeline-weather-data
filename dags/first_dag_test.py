from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
            'my_first_dag_test_1',
            start_date=days_ago(2),
            schedule_interval='@daily' #this dag will start to execute at midnight
) as dag:

    task_one = EmptyOperator(task_id = 'task_one_test')
    task_two = EmptyOperator(task_id = 'task_two_test')
    task_three = EmptyOperator(task_id = 'task_three_test')
    task_four = BashOperator(
        task_id = 'create_new_folder',
        bash_command='mkdir -p "/home/gustavo/Documents/airflowpipeline/folder_created_by_airflow={{ data_interval_end }}"' #mkdir -p just because if this folder already exists, nothing gonna happen - bash_command for write the command you'd like to execute 
    )

    task_one >> [task_two, task_three]
    task_three >> task_four