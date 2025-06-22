import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import logging

class WeatherAPIHook(BaseHook):
    def __init__(self, currency_conn_id: str):
        super().__init__()
        self.conn_id = currency_conn_id

    
    def get_weather_data(self, city: str):
            """
            Получение данных о погоде для заданного города
            
            Args:
                city (str): Название города на английском языке
                
            Returns:
                dict: Словарь с температурой и состоянием погоды
                    {"temp_c": <температура>, "condition": <состояние>}
            """
            url = self._get_host()
            # Форматирование города: убираем лишние пробелы и приводим к правильному формату
            formatted_city = city.strip().lower().title()
            
            params = {
                'key': self._get_api_key(),
                'q': formatted_city,
            }
            
            logging.info(f"Запрос погоды для города: {formatted_city}")
            logging.info(f"URL: {url}")
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                full_data = response.json()
                logging.info(f"Получены данные для {formatted_city}")
                
                weather_result = {
                    "temp_c": full_data['current']['temp_c'],
                    "condition": full_data['current']['condition']['text']
                }
                
                logging.info(f"Результат: {weather_result}")
                return weather_result
                
            except requests.exceptions.HTTPError as e:
                logging.error(f"HTTP ошибка для города {formatted_city}: {e}")
                raise AirflowException(f"Failed to get weather for {formatted_city}: {e}")
            except KeyError as e:
                logging.error(f"Ошибка структуры ответа для {formatted_city}: {e}")
                raise AirflowException(f"Unexpected response structure for {formatted_city}: {e}")
            except Exception as e:
                logging.error(f"Общая ошибка для города {formatted_city}: {e}")
                raise AirflowException(f"Error getting weather for {formatted_city}: {e}")


    def _get_api_key(self):
        """Получение API ключа из настроек подключения"""
        conn = self.get_connection(self.conn_id)
        if not conn.password:
            raise AirflowException('Missing API key (password) in connection settings')
        return conn.password
    
    def _get_host(self):
        """Получение URL API из настроек подключения"""
        conn = self.get_connection(self.conn_id)
        if not conn.host:
            raise AirflowException('Missing HOST in connection settings')
        return conn.host