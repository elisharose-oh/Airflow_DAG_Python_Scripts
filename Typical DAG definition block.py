#Typical DAG definition block
#import the libraries

from datetime import timedelta
#The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments #DAG settings #You can override them on a per-task basis during operator intialization

default_args = {
    'owner': 'Your username',#the owner name
    'start_date': days_ago(0),#when this DAG should run from: days_ago(0) means today
    'email': ['youremailaddr.com'],#the email address where the alerts are sent to
    'email_on_failure': True,#whether alert must be sent on failure
    'email_on_retry': True,#whether alert must be sent on retry
    'retries': 1,#number of retries in case of failure
    'retry_delay': timedelta(minutes=5),#the time delay between retries

}
#define the DAG
dag = DAG(
    dag_id = 'sample-etl-dag',#ID of the DAG#this is what you see on web console
    default_args=default_args,#Passing the dictionary default_args, in which all defaults are defined
    description = 'Sample ETL DAG using Bash',#description helps us in understanding what this DAG does
    schedule_interval-timedelta(days=1),#schedule interval tells us how frequently this DAG runs. In this case everyday.
    
)
#define the tasks

#define the first task named extract
extract = BashOperator(
    task_id = 'extract',#task_id is a string and helps in identifying the task.
    bash_command = 'echo "extract"',#defines what bash command the task represents
    dag = dag,#defines which dag this task belongs to
)

#define the second task named transform
transform = BashOperator(
    task_id = 'transform',
    bash_command = 'echo "transform"',
    dag=dag,
)

#define the third task named load
load = BashOperator(
    task_id = 'load',
    bash_command = 'echo "load"',
    dag = dag,
)
#task pipeline#helps to organize the order of tasks
extract >> transform >>load#here the task extract must run first, followed by transform, followed by the task load.

