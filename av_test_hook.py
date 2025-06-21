from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Импорт вашего хука
from hooks.CurrencyWeatherHook import CurrencyWeatherHook
    
def get_weather(city='Moscow'):
    """Получение погоды"""
    # Создаем хук
    hook = CurrencyWeatherHook(currency_conn_id='weather_api')
    
    # Получаем данные
    result = hook.get_weather_data(city=city)
    
    # Выводим результат
    print(f"Погода в Москве: {result}")
    return result


with DAG(
    'weather',
    schedule_interval=None,  # Ручной запуск
    start_date=datetime(2025, 6, 20)  
) as dag:
    
    # Задача для получения погоды в Москве
    moscow_task = PythonOperator(
        task_id="get_moscow_weather", 
        python_callable=get_weather,
        dag=dag,
    )