import datetime as dt
import os
from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email_smtp
from textwrap import dedent
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

import warnings

warnings.filterwarnings("ignore")

date_today = dt.datetime.today()
dag_id = "clear_logs"


email_notification_list = [
    "DVEroshevich@sber-bank.by",
]

args = {
    "owner": "BICC",
    "start_date": dt.datetime(2024, 8, 25),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=2),
    "depends_on_past": False,
    "email": email_notification_list,
    "email_on_failure": True,
}


def filter_content(**kwargs):
    folders = kwargs['ti'].xcom_pull(task_ids='check_logs_content')
    folders_list = folders.replace(",latest,", "").split(',')
    unnecessary_folders = ""
    for item in folders_list:
        item_obj = dt.datetime.strptime(item, "%Y-%m-%d")
        if item_obj < date_today - dt.timedelta(days=10):
            unnecessary_folders = unnecessary_folders + " " + item
    kwargs['ti'].xcom_push(key='unnecessary_folders', value=unnecessary_folders)


with DAG(
        dag_id=dag_id, default_args=args, schedule_interval="0 8 1 * *"
) as dag:  # run monthly at 8:00 AM
    
    check_logs_content = BashOperator(
        task_id="check_logs_content",
        bash_command="ls -1 /home/airflow/airflow_services/logs/scheduler | tr '\n', ','",
        do_xcom_push=True,
        dag=dag,
    )

    filter_content = PythonOperator(
        task_id="filter_content",
        python_callable=filter_content,
        provide_context=True,
        dag=dag,
    )

    clear_logs = BashOperator(
        task_id='clear_logs',
        bash_command='cd /home/airflow/airflow_services/logs/scheduler && rm -rf {{ ti.xcom_pull(task_ids=\'filter_content\', key=\'unnecessary_folders\') }} ',
        dag=dag,
    )
    
    email_notification = EmailOperator(
        task_id="email_notification",
        dag=dag,
        to=email_notification_list,
        subject="Job {{ dag.dag_id }} : Completed",
        html_content=dedent(
            """
                          <b>Задание:</b> {{ dag.dag_id }} <br><br>
                          <b>Статус:</b>Успешно выполнено <br><br>
                          <b>Дата:</b> {{params.date}}<br><br>
                          <b>Host:</b> 10.244.226.126 prom_etl_ds.belpsb.by <br><br>
                          <b>Logs:</b>: /ml_data/models_logs/{{dag.dag_id}}/<br>
                          """
        ),
        params={"date": dt.datetime.now().strftime("%Y_%m_%d %H:%M")},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

check_logs_content >> filter_content >> clear_logs >> email_notification
