import pandas as pd
import logging
import os
from datetime import datetime
from typing import Any, Dict

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


from hooks.weather_api_hook import CurrencyWeatherHook

class CurrencyWeatherOperator(BaseOperator):
    """
    Оператор для получения данных о погоде и сохранения их в CSV через Pandas
    
    :param city: Название города для получения погоды
    :param file_path: Путь для сохранения CSV файла
    :param conn_id: ID подключения для Weather API
    """


    @apply_defaults
    def __init__(
            self,
        city: str,
        file_path: str,
        conn_id: str = 'weather_api',
        **kwargs
            ) -> None:
        super().__init__(**kwargs)
        self.city = city
        self.file_path = file_path
        self.conn_id = conn_id


    def execute(self, context: Any):
        """
        Основной метод выполнения оператора:
        1. Получает данные о погоде через Hook
        2. Преобразует в Pandas DataFrame
        3. Сохраняет в CSV файл
        """
        logging.info(f"=== Начинаем выполнение CurrencyWeatherOperator ===")
        logging.info(f"Город: {self.city}")
        logging.info(f"Файл: {self.file_path}")
        
        # 1. Получаем данные о погоде через Hook
        
        weather_hook = CurrencyWeatherHook(currency_conn_id=self.conn_id)
        weather_data = weather_hook.get_weather_data(city=self.city)
        
        logging.info(f"Получены данные о погоде: {weather_data}")

        # 2. Преобразуем данные в Pandas DataFrame
        
        df = self._create_dataframe(weather_data)
        
        # 3. Сохраняем DataFrame в CSV файл
        
        self._save_to_csv(df)
        
        logging.info(f"=== Выполнение завершено успешно ===")

        return {
            'city': self.city,
            'file_path': self.file_path,
            'records_saved': len(df),
            'weather_data': weather_data,
            'execution_time': datetime.now().isoformat()
        }
    
    def _create_dataframe(self, weather_data: Dict[str, Any]) -> pd.DataFrame:
        """Создание Pandas DataFrame из данных о погоде"""
        current_time = datetime.now()
        
        df_data = {
            'city': [self.city],
            'temp_c': [weather_data['temp_c']],
            'condition': [weather_data['condition']],
            'timestamp': [current_time.isoformat()],
            'date': [current_time.strftime('%Y-%m-%d')],
            'time': [current_time.strftime('%H:%M:%S')]
        }
        
        df = pd.DataFrame(df_data)
        
        logging.info(f"DataFrame создан с {len(df)} записями")
        logging.info(f"Колонки: {list(df.columns)}")
        
        return df
    

    def _save_to_csv(self, df: pd.DataFrame) -> None:
        """Сохранение DataFrame в CSV файл"""
        try:
            # Создаем директорию если не существует
            directory = os.path.dirname(self.file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                logging.info(f"Создана директория: {directory}")
            
            # Сохраняем DataFrame в CSV
            df.to_csv(
                self.file_path,
                index=False,
                encoding='utf-8',
                sep=','
            )
            
            logging.info(f"CSV файл успешно сохранен: {self.file_path}")
            
            # Проверяем размер файла
            file_size = os.path.getsize(self.file_path)
            logging.info(f"Размер файла: {file_size} байт")
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении файла: {e}")
            raise