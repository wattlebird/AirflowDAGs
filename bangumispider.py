from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


dag = DAG(
    dag_id='ScheduledBangumiSpiderTask',
    start_date=datetime(2017, 5, 1),
    end_date=datetime(2018, 5, 1),
    schedule_interval="0 6 15 * *",
)

fin = DummyOperator(task_id='grouper', dag=dag)

record_task = BashOperator(
    task_id='record',
    bash_command='curl http://ikely.me:6800/schedule.json -d project=bgm -d spider=record -d id_max=500000',
    dag=dag)
record_task.set_downstream(fin)

subject_task = BashOperator(
    task_id='subject',
    bash_command='curl http://ikely.me:6800/schedule.json -d project=bgm -d spider=subject -d id_max=300000',
    dag=dag)
subject_task.set_downstream(fin)

if __name__ == "__main__":
    dag.cli()