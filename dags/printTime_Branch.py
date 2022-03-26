import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
import datetime
import math

switcher = {
    1: 'branch-0-10',
    2: 'branch-11-20',
    3: 'branch-21-30',
    4: 'branch-31-40',
    5: 'branch-41-50',
    6: 'branch-51-60',
}


def choose_branch_one():
    now = datetime.datetime.now()
    return switcher.get(math.ceil(now.minute/10), 1)


with DAG(
    dag_id='example_branch_operator',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=['Dummy', 'Branch'],
) as dag:
    run_this_first = DummyOperator(
        task_id='run_this_first',
    )

    options = list(switcher.values())

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch_one,
    )
    run_this_first >> branching

    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for option in options:
        t = DummyOperator(
            task_id=option,
        )

        dummy_follow = DummyOperator(
            task_id='follow_' + option,
        )

        branching >> t >> dummy_follow >> join
    run_this_first
