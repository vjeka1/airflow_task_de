from datetime import datetime
from airflow import DAG
from operators import WeatherToCSVOperator

# Список городов для получения погоды
cities = ["Moscow", "Paris", "Berlin"]

with DAG(
    'test_weather_dag_cities',
    schedule_interval=None,
    start_date=datetime(2025, 6, 19),
    description='Получение погоды для нескольких городов',
    catchup=False
) as dag:

    # Создаем задачу для каждого города
    for city in cities:
        weather_task = WeatherToCSVOperator(
            task_id=f"save_{city.lower()}_weather",
            city=city,
            file_path=f'/opt/airflow/data/tmp/{city.lower()}_weather.csv',
            conn_id='weather_api',
            dag=dag,
        )