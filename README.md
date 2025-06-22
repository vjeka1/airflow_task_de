# Airflow Weather API Project

Проект для работы с Weather API в Apache Airflow. Включает в себя создание собственного Hook и Operator для получения и обработки данных о погоде.

## Структура проекта

```
dags/
├── hooks/
│   ├── __init__.py
│   └── weather_api_hook.py          # Hook для Weather API
├── operators/
│   ├── __init__.py
│   └── weather_pandas_operator.py   # Operator для работы с pandas DataFrame
└── test_weather_dag_cities.py       # Отдельный тест городов (опционально)
```

## Компоненты

### 1. CurrencyWeatherHook

Собственный Hook для взаимодействия с Weather API.

**Возможности:**
- Получение данных о погоде по названию города
- Возврат температуры и состояния погоды в формате JSON
- Обработка ошибок и логирование

**Пример использования:**
```python
from hooks import CurrencyWeatherHook
hook = CurrencyWeatherHook(currency_conn_id='weather_api')
result = hook.get_weather_data(city=city)
# Результат: {'temp_c': 14.4, 'condition': 'Partly Cloudy'}
```

### 2. CurrencyWeatherOperator

Operator для получения данных о погоде и сохранения их в CSV через pandas DataFrame.

**Возможности:**
- Получение данных через WeatherHook
- Преобразование в pandas DataFrame
- Сохранение в CSV файл с дополнительными метаданными

**Пример использования:**
```python
from operators import CurrencyWeatherOperator

weather_task = CurrencyWeatherOperator(
    task_id="save_moscow_weather",
    city='Moscow',
    file_path='/opt/airflow/data/tmp/moscow_weather.csv',
    conn_id='weather_api',
    dag=dag,
)
```

## Быстрый старт

### 1. Получите API ключ
1. Зарегистрируйтесь на [WeatherAPI.com](https://www.weatherapi.com/)
2. Получите бесплатный API ключ

### 2. Настройте подключение в Airflow
1. Откройте Airflow Web UI
2. Перейдите в **Admin → Connections → Add**
3. Заполните поля:
   - **Connection Id:** `weather_api`
   - **Connection Type:** `HTTP`
   - **Host:** `https://api.weatherapi.com/v1/current.json`
   - **Password:** `ваш_api_ключ`
4. Нажмите **Save**

### 3. Установите зависимости

```bash
pip install pandas requests
```

### 4. Запустите тестирование 

#### Через CLI

```bash
airflow dags test test_weather_dag_cities 2025-06-20
```

## Результаты выполнения

После успешного выполнения создаются CSV файлы:
- `/opt/airflow/data/tmp/moscow_weather.csv` - данные о Москве  
- `/opt/airflow/data/tmp/paris_weather.csv` - данные о Париже
- `/opt/airflow/data/tmp/berlin_weather.csv` - данные о Берлине

Каждый файл содержит:
- Название города
- Температура
- Состояние погоды
- Временные метки
- Метаданные о выполнении

## Структура CSV файла

```csv
city,temp_c,condition,timestamp,date,time
Moscow,14.4,Partly cloudy,2025-06-20T14:30:00,2025-06-20,14:30:00
```
