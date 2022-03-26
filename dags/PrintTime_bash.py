from textwrap import dedent
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


my_dag_id = "printTime_Bash"

default_args = {
    'owner': 'saireddyavs',
    'depends_on_past': False,
    'retries': 10,
    'concurrency': 1
}


dag = DAG(
    dag_id=my_dag_id,
    default_args=default_args,
    start_date=datetime(2020, 3, 17),
    schedule_interval=timedelta(seconds=10)
)

templated_command = dedent(
    """
    echo "current time is `date +%Y-%m-%d-%H:%M:%S` ⌛️"
    """
)

bash_task = BashOperator(task_id='bash_task_1',
                         bash_command=templated_command,
                         dag=dag)

bash_task
