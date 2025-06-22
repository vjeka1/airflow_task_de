from datetime import datetime
from airflow import DAG
from operators.weather_pandas_operator import CurrencyWeatherOperator

with DAG(
    'test_weather_dag_operator',
    schedule_interval=None,  # Ручной запуск
    start_date=datetime(2025, 6, 20)  
) as dag:
    
    """Получение погоды"""
    weather_task = CurrencyWeatherOperator(
        task_id="save_moscow_weather",
        city='Moscow',
        file_path='/opt/airflow/data/tmp/moscow_weather.csv',
        conn_id='weather_api',
        dag=dag,
    )