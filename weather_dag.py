from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import json
import os

from weather_script import run_weather_etl, check_temperature_alerts, send_email_alert 

# Se definen argumentos

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Se define DAG con Backfill

dag = DAG(
    'daily_weather_etl',
    default_args=default_args,
    description='A DAG to fetch weather data, check for temperature alerts, and send email notifications',
    schedule_interval=timedelta(days=1),
    catchup=True,
) 

# Primer tarea para comenzar toma de datos, conexion y carga a Redshift. Se utiliza XCom para vincular data entre tareas

def task_run_weather_etl(**kwargs):

    logging.info("Starting task_run_weather_etl")

    weather_data_dict = run_weather_etl()
    kwargs['ti'].xcom_push(key='weather_data', value=weather_data_dict)

# Segunda tarea para chequear alertas

def task_check_temperature_alerts(**kwargs):

    logging.info("Starting task_check_temperature_alerts")

    ti = kwargs['ti']
    weather_data_dict = ti.xcom_pull(key='weather_data', task_ids='run_weather_etl')
    weather_data_df = pd.DataFrame(weather_data_dict)

    # Convertir columnas necesarias a su tipo adecuado
    weather_data_df['temp2m'] = pd.to_numeric(weather_data_df['temp2m'])
    weather_data_df['timestamp'] = pd.to_datetime(weather_data_df['timestamp'])

    # Cargar configuración desde archivo JSON
    file_path = os.path.join(os.path.dirname(__file__),'config_alertas.json')
    with open(file_path) as config_file:
        config = json.load(config_file)

    temp_limits = config['temperature_limits']

    alerts = check_temperature_alerts(weather_data_df, temp_limits)
    kwargs['ti'].xcom_push(key='alerts', value=alerts)

# Tercer tarea para enviar alertas por mail

def task_send_email_alert(**kwargs):

    logging.info("Starting task_send_email_alert")

    ti = kwargs['ti']
    alerts = ti.xcom_pull(key='alerts', task_ids='check_temperature_alerts')
    logging.info(f"alerts: {alerts}")

    if alerts:
        # Cargar configuración desde archivo JSON
        file_path = os.path.join(os.path.dirname(__file__),'config_alertas.json')
        with open(file_path) as config_file:
            config = json.load(config_file)

        email_settings = config['email_settings']
        send_email_alert(alerts, email_settings)

run_weather_etl_task = PythonOperator(
    task_id='run_weather_etl',
    python_callable=task_run_weather_etl,
    provide_context=True,
    dag=dag,
)

check_temperature_alerts_task = PythonOperator(
    task_id='check_temperature_alerts',
    python_callable=task_check_temperature_alerts,
    provide_context=True,
    dag=dag,
)

send_email_alert_task = PythonOperator(
    task_id='send_email_alert',
    python_callable=task_send_email_alert,
    provide_context=True,
    dag=dag,
)

run_weather_etl_task >> check_temperature_alerts_task >> send_email_alert_task
