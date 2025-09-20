import pandas as pd
import json
import re
from datetime import datetime, timedelta
import numpy as np
import argparse
import os
import logging
import sys
import glob
import psycopg2
from psycopg2.extras import execute_values
import xlrd

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация БД
DB_CONFIG = {
    'dbname': 'bpla_db',
    'user': 'postgres',
    'password': '15321',  # Измените на реальный пароль
    'host': 'localhost',
    'port': '5432'
}

class BPLAParser:
    """Парсер для обработки файлов с данными о полетах БПЛА в соответствии с Табелем сообщений (приказ Минтранса России от 24.01.2013 №13)"""
    
    # Расширенный список регионов РФ с bounding boxes (добавлено больше субъектов для полноты)
    # Для точной геопривязки в БД используем PostGIS, но здесь для предварительного определения
    RUSSIAN_REGIONS = {
        'Московская область': {'min_lat': 54.2, 'max_lat': 56.9, 'min_lon': 35.2, 'max_lon': 40.2},
        'Смоленская область': {'min_lat': 53.9, 'max_lat': 55.9, 'min_lon': 30.8, 'max_lon': 35.3},
        'Тверская область': {'min_lat': 55.6, 'max_lat': 58.8, 'min_lon': 31.3, 'max_lon': 37.9},
        'Ярославская область': {'min_lat': 56.5, 'max_lat': 58.7, 'min_lon': 37.4, 'max_lon': 41.3},
        'Владимирская область': {'min_lat': 55.1, 'max_lat': 56.8, 'min_lon': 38.3, 'max_lon': 42.4},
        'Рязанская область': {'min_lat': 53.3, 'max_lat': 55.5, 'min_lon': 38.3, 'max_lon': 42.4},
        'Тульская область': {'min_lat': 53.1, 'max_lat': 54.8, 'min_lon': 35.8, 'max_lon': 38.9},
        'Калужская область': {'min_lat': 53.3, 'max_lat': 55.4, 'min_lon': 33.3, 'max_lon': 37.3},
        'Нижегородская область': {'min_lat': 54.4, 'max_lat': 58.0, 'min_lon': 41.4, 'max_lon': 47.7},
        'Ленинградская область': {'min_lat': 58.5, 'max_lat': 61.2, 'min_lon': 28.0, 'max_lon': 35.0},
        'Санкт-Петербург': {'min_lat': 59.6, 'max_lat': 60.2, 'min_lon': 29.5, 'max_lon': 30.8},
        'Новгородская область': {'min_lat': 56.8, 'max_lat': 59.7, 'min_lon': 29.9, 'max_lon': 35.2},
        'Псковская область': {'min_lat': 55.8, 'max_lat': 59.0, 'min_lon': 27.3, 'max_lon': 31.5},
        'Брянская область': {'min_lat': 52.1, 'max_lat': 54.0, 'min_lon': 31.1, 'max_lon': 35.2},
        'Орловская область': {'min_lat': 52.0, 'max_lat': 53.6, 'min_lon': 35.0, 'max_lon': 37.5},
        'Липецкая область': {'min_lat': 51.8, 'max_lat': 53.5, 'min_lon': 38.0, 'max_lon': 40.5},
        'Воронежская область': {'min_lat': 49.5, 'max_lat': 52.2, 'min_lon': 38.0, 'max_lon': 42.0},
        'Белгородская область': {'min_lat': 49.9, 'max_lat': 51.5, 'min_lon': 35.5, 'max_lon': 39.0},
        'Курская область': {'min_lat': 51.0, 'max_lat': 52.5, 'min_lon': 34.0, 'max_lon': 37.0},
        'Республика Карелия': {'min_lat': 60.5, 'max_lat': 66.5, 'min_lon': 29.0, 'max_lon': 38.0},
        'Ростовская область': {'min_lat': 46.2, 'max_lat': 50.0, 'min_lon': 38.0, 'max_lon': 44.0},
        'Астраханская область': {'min_lat': 45.0, 'max_lat': 48.5, 'min_lon': 45.0, 'max_lon': 49.0},
        'Новосибирская область': {'min_lat': 53.0, 'max_lat': 57.0, 'min_lon': 75.0, 'max_lon': 85.0},
        'Красноярский край': {'min_lat': 54.0, 'max_lat': 77.0, 'min_lon': 77.0, 'max_lon': 108.0},
        'Тюменская область': {'min_lat': 55.0, 'max_lat': 61.0, 'min_lon': 64.0, 'max_lon': 77.0},
        'Свердловская область': {'min_lat': 56.0, 'max_lat': 61.0, 'min_lon': 57.0, 'max_lon': 66.0},
        'Республика Алтай': {'min_lat': 50.0, 'max_lat': 52.0, 'min_lon': 85.0, 'max_lon': 89.0},
        # Добавьте другие регионы по мере необходимости
    }
    
    def __init__(self, db_conn):
        self.processed_count = 0
        self.error_count = 0
        self.db_conn = db_conn
        self.cur = db_conn.cursor()
        
    def _parse_coordinates(self, coord_str: str):
        """Усовершенствованный парсинг координат с дополнительными паттернами"""
        if not coord_str or pd.isna(coord_str) or str(coord_str).strip() in ['', 'nan', 'None']:
            return None
            
        coord_str = str(coord_str).upper().replace(' ', '').replace('"', '').replace(',', '').replace('.', '')
        
        patterns = [
            r'(\d{2})(\d{2})([NS])(\d{3})(\d{2})([EW])',
            r'(\d{2})(\d{2})([NS])(\d{2})(\d{2})([EW])',
            r'(\d{4})([NS])(\d{5})([EW])',
            r'(\d{4})([NS])(\d{4})([EW])',
            r'(\d{1,3}\.?\d{0,6})([NS])(\d{1,3}\.?\d{0,6})([EW])',
            r'(\d{2})\s*(\d{2})\s*([NS])\s*(\d{3})\s*(\d{2})\s*([EW])',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, coord_str)
            if match:
                groups = match.groups()
                try:
                    if len(groups) == 6:
                        lat_deg, lat_min, lat_dir, lon_deg, lon_min, lon_dir = groups
                        lat = float(lat_deg) + float(lat_min) / 60
                        lon = float(lon_deg) + float(lon_min) / 60
                    elif len(groups) == 4:
                        lat_str, lat_dir, lon_str, lon_dir = groups
                        if '.' in lat_str:
                            lat = float(lat_str)
                        else:
                            lat = float(lat_str[:2]) + float(lat_str[2:]) / 60
                        if '.' in lon_str:
                            lon = float(lon_str)
                        else:
                            lon = float(lon_str[:3]) + float(lon_str[3:]) / 60 if len(lon_str) > 4 else float(lon_str[:2]) + float(lon_str[2:]) / 60
                    else:
                        continue
                    
                    if lat_dir == 'S': lat = -lat
                    if lon_dir == 'W': lon = -lon
                    
                    if 40 < lat < 80 and 19 < lon < 180:
                        return [round(lon, 6), round(lat, 6)]
                    else:
                        logger.warning(f"Невалидные координаты: {lat}, {lon}")
                        return None
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Ошибка парсинга координат {coord_str}: {e}")
                    continue
                    
        logger.warning(f"Не удалось распарсить координаты: {coord_str}")
        return None
    
    def _parse_flight_id(self, text: str):
        """Усовершенствованный парсинг ID с дополнительными паттернами"""
        if not text or pd.isna(text):
            return None  # Теперь возвращаем None для неопределенных
            
        text = str(text)
        
        patterns = [
            r'REG[ /]?([A-Z0-9\-]+)',
            r'SHR-([A-Z0-9\-]+)',
            r'\((FPL|SHR)-([A-Z0-9\-]+)',
            r'ID[ /]?([A-Z0-9\-]+)',  # Дополнительный
            r'([A-Z]{3}\d{4})',  # Пример ICAO-like
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1) if len(match.groups()) > 1 else match.group(0)
            
        logger.warning(f"Не удалось распарсить ID: {text}")
        return None
    
    def _parse_uav_type(self, text: str):
        """Усовершенствованный парсинг типа с дополнительными ключевыми словами"""
        if not text or pd.isna(text):
            return None
            
        text = str(text).upper()
        
        type_keywords = {
            'MERLIN': 'MERLIN-21B',
            'CINEBOT': 'GEPRC CINEBOT30',
            'DJI': 'DJI',
            'MAVIC': 'DJI MAVIC',
            'SKYHANTER': 'SKYHANTER',
            'BLA': 'BLA',
            'БПЛА': 'BLA',
            'БВС': 'BVS',
            'QUADCOPTER': 'QUADCOPTER',
            'DRONE': 'DRONE',
            'PHANTOM': 'DJI PHANTOM',
            'INSPIRE': 'DJI INSPIRE',
            'MATRICE': 'DJI MATRICE',
            'AUTEL': 'AUTEL',
            'PARROT': 'PARROT',
        }
        
        for keyword, uav_type in type_keywords.items():
            if keyword in text:
                return uav_type
                
        type_match = re.search(r'TYP[ /]?([A-Z0-9\-]+)', text)
        if type_match:
            return type_match.group(1)
            
        type_match = re.search(r'-([A-Z0-9]+/[HMLJ])', text)
        if type_match:
            return type_match.group(1)
            
        logger.warning(f"Не удалось распарсить тип: {text}")
        return None
    
    def _parse_time(self, text: str, pattern: str):
        match = re.search(pattern, text.upper())
        if match:
            time_str = match.group(1)
            try:
                return datetime.strptime(time_str, '%H%M').time()
            except:
                try:
                    return datetime.strptime(time_str, '%H%M%S').time()
                except:
                    pass
        return None
    
    def _parse_eet(self, text: str):
        match = re.search(r'(EET|DOF|RMK/EET)?[ /]?(\d{2,4})', text.upper())
        if match:
            eet_str = match.group(2)
            try:
                if len(eet_str) == 4:
                    hours = int(eet_str[:2])
                    minutes = int(eet_str[2:])
                elif len(eet_str) == 2:
                    hours = 0
                    minutes = int(eet_str)
                else:
                    hours = int(eet_str) // 60
                    minutes = int(eet_str) % 60
                return timedelta(hours=hours, minutes=minutes)
            except:
                pass
        return timedelta(hours=1)
    
    def _extract_coordinates_from_text(self, text: str):
        if not text or pd.isna(text):
            return None
            
        text = str(text)
        
        coord_patterns = [
            r'DEP[ /]?([0-9NSWE\.]+)',
            r'DEST[ /]?([0-9NSWE\.]+)',
            r'(\d{4,5}[NS]\d{4,6}[EW])',
            r'ALTN[ /]?([0-9NSWE\.]+)',
            r'RMK[ /]?COORD[ /]?([0-9NSWE\.]+)',
            r'([NS]\d{1,2}\.?\d{0,6}\s*[EW]\d{1,3}\.?\d{0,6})',
        ]
        
        for pattern in coord_patterns:
            matches = re.findall(pattern, text.upper())
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                coords = self._parse_coordinates(match)
                if coords:
                    return coords
                    
        return None
    
    def _determine_region(self, coordinates):
        if not coordinates or len(coordinates) != 2:
            return None
            
        lon, lat = coordinates
        
        for region_name, bbox in self.RUSSIAN_REGIONS.items():
            if (bbox['min_lat'] <= lat <= bbox['max_lat'] and 
                bbox['min_lon'] <= lon <= bbox['max_lon']):
                return region_name
                
        logger.warning(f"Регион не определен для координат: {coordinates}")
        return None

    def parse_excel_file(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл {file_path} не найден")
            
        try:
            engine = 'openpyxl' if file_path.endswith('.xlsx') else 'xlrd'
            try:
                # Try reading with the default encoding
                df = pd.read_excel(file_path, header=None, engine=engine)
                logger.info(f"Успешно загружен {file_path} с движком {engine}")
            except Exception as e:
                logger.warning(f"Ошибка чтения {file_path} с движком {engine}: {e}")
                # Fallback: clean the file or try alternative reading method
                try:
                    # Read as text and clean encoding issues
                    with open(file_path, 'rb') as f:
                        content = f.read().decode('utf-8', errors='ignore')
                    # If it's not a valid Excel file, log and skip
                    if not file_path.endswith(('.xlsx', '.xls')):
                        logger.error(f"Файл {file_path} не является Excel-файлом")
                        return []
                    # Try reading again with openpyxl without encoding_errors
                    df = pd.read_excel(file_path, header=None, engine=engine)
                    logger.info(f"Успешно загружен {file_path} с движком {engine} после повторной попытки")
                except Exception as e:
                    logger.error(f"Не удалось прочитать {file_path} после всех попыток: {e}")
                    return []
            
            logger.info(f"Загружено {len(df)} строк из {file_path}")
            
            header_row = None
            for i in range(min(20, len(df))):
                row_values = df.iloc[i].astype(str).str.upper()
                if any(x in row_values.values for x in ['ДАТА ПОЛЁТА', 'SHR', 'DEP', 'ARR']):
                    header_row = i
                    break
            
            if header_row is None:
                df = pd.read_excel(file_path, header=0, engine=engine)
            else:
                df = pd.read_excel(file_path, header=header_row, engine=engine)
            
            # Clean string columns to handle encoding issues
            df = df.apply(lambda x: x.str.encode('utf-8', errors='ignore').str.decode('utf-8') if x.dtype == "object" else x)
            
            flights_data = []
            
            for index, row in df.iterrows():
                try:
                    if all(pd.isna(val) for val in row) or 'ДАТА ПОЛЁТА' in str(row.iloc[0]).upper():
                        continue
                    
                    columns = row.to_dict()
                    date_key = next((k for k in columns if 'ДАТА' in str(k).upper()), 0)
                    shr_key = next((k for k in columns if 'SHR' in str(k).upper() or 'ТЕКСТ' in str(k).upper()), 1)
                    dep_key = next((k for k in columns if 'DEP' in str(k).upper() or 'ВЗЛЕТ' in str(k).upper()), 2)
                    arr_key = next((k for k in columns if 'ARR' in str(k).upper() or 'ПОСАДКА' in str(k).upper()), 3)
                    
                    date_value = columns.get(date_key)
                    shr_text = str(columns.get(shr_key, ""))
                    dep_text = str(columns.get(dep_key, ""))
                    arr_text = str(columns.get(arr_key, ""))
                    
                    if pd.isna(date_value):
                        base_date = datetime.now().date()
                    else:
                        try:
                            if isinstance(date_value, datetime):
                                base_date = date_value.date()
                            else:
                                formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y', '%m/%d/%Y']
                                for fmt in formats:
                                    try:
                                        base_date = datetime.strptime(str(date_value), fmt).date()
                                        break
                                    except:
                                        continue
                                else:
                                    base_date = datetime.now().date()
                        except:
                            base_date = datetime.now().date()
                    
                    flight_id = self._parse_flight_id(shr_text + dep_text + arr_text)
                    if not flight_id:
                        logger.warning(f"Пропущена строка {index} в {file_path}: ID не найден")
                        continue
                    
                    uav_type = self._parse_uav_type(shr_text + dep_text + arr_text)
                    if not uav_type:
                        uav_type = "UNKNOWN"
                    
                    dep_coords = self._extract_coordinates_from_text(dep_text + shr_text + arr_text)
                    arr_coords = self._extract_coordinates_from_text(arr_text + shr_text + dep_text)
                    if not dep_coords and not arr_coords:
                        logger.warning(f"Пропущена строка {index} в {file_path}: координаты не найдены")
                        continue
                    
                    if not arr_coords and dep_coords:
                        arr_coords = dep_coords
                    elif not dep_coords and arr_coords:
                        dep_coords = arr_coords
                    
                    dep_time = self._parse_time(shr_text + dep_text, r'(?:DEP|ETD|TAKEOFF)[ /]?[A-Z0-4]{0,4}(\d{4,6})') or datetime.now().time()
                    eet = self._parse_eet(shr_text + arr_text + dep_text)
                    arr_time = (datetime.combine(base_date, dep_time) + eet).time()
                    
                    dep_datetime = datetime.combine(base_date, dep_time).isoformat()
                    arr_datetime = datetime.combine(base_date, arr_time).isoformat()
                    duration_minutes = eet.total_seconds() / 60
                    
                    region = self._determine_region(dep_coords) if dep_coords else None
                    if not region:
                        region = "Не определен"
                    
                    flight_data = {
                        'flight_id': flight_id,
                        'uav_type': uav_type,
                        'departure_coordinates': dep_coords,
                        'arrival_coordinates': arr_coords,
                        'departure_time': dep_datetime,
                        'arrival_time': arr_datetime,
                        'duration_minutes': duration_minutes,
                        'region': region,
                        'source_file': os.path.basename(file_path),
                        'row_index': index + 1,
                        'parse_date': datetime.now().isoformat()
                    }
                    
                    flights_data.append(flight_data)
                    self.processed_count += 1
                    
                except Exception as e:
                    self.error_count += 1
                    logger.error(f"Ошибка в строке {index} файла {file_path}: {e}")
                    continue
            
            return flights_data
            
        except Exception as e:
            logger.error(f"Ошибка чтения {file_path}: {e}")
            return []
    
    def insert_to_db(self, flights_data):
        """Вставка данных в нормализованную БД"""
        if not flights_data:
            logger.warning("Нет данных для вставки в БД")
            return
        
        uav_types = list(set(f['uav_type'] for f in flights_data if f['uav_type']))
        regions = list(set(f['region'] for f in flights_data if f['region']))
        
        if uav_types:
            execute_values(self.cur, """
                INSERT INTO uav_types (type_name) VALUES %s
                ON CONFLICT (type_name) DO NOTHING
            """, [(t,) for t in uav_types])
        
        if regions:
            execute_values(self.cur, """
                INSERT INTO regions (region_name) VALUES %s
                ON CONFLICT (region_name) DO NOTHING
            """, [(r,) for r in regions])
        
        self.cur.execute("SELECT type_name, id FROM uav_types WHERE type_name IN %s", (tuple(uav_types),))
        type_ids = dict(self.cur.fetchall())
        
        self.cur.execute("SELECT region_name, id FROM regions WHERE region_name IN %s", (tuple(regions),))
        region_ids = dict(self.cur.fetchall())
        
        flight_tuples = []
        for f in flights_data:
            dep_point = f"POINT({f['departure_coordinates'][0]} {f['departure_coordinates'][1]})" if f['departure_coordinates'] else None
            arr_point = f"POINT({f['arrival_coordinates'][0]} {f['arrival_coordinates'][1]})" if f['arrival_coordinates'] else None
            type_id = type_ids.get(f['uav_type'])
            region_id = region_ids.get(f['region'])
            
            flight_tuples.append((
                f['flight_id'],
                type_id,
                dep_point,
                arr_point,
                f['departure_time'],
                f['arrival_time'],
                f['duration_minutes'],
                region_id,
                f['source_file'],
                f['row_index'],
                f['parse_date']
            ))
        
        if flight_tuples:
            try:
                execute_values(self.cur, """
                    INSERT INTO flights (
                        flight_id, uav_type_id, departure_point, arrival_point,
                        departure_time, arrival_time, duration_minutes, region_id,
                        source_file, row_index, parse_date
                    ) VALUES %s
                    ON CONFLICT (flight_id, departure_time) DO NOTHING
                """, flight_tuples)
                self.db_conn.commit()
                logger.info(f"Вставлено {len(flight_tuples)} записей в БД")
            except Exception as e:
                logger.error(f"Ошибка вставки в БД: {e}")
                self.db_conn.rollback()

def validate_db_config(config):
    """Validate the database configuration parameters."""
    required_keys = ['dbname', 'user', 'password', 'host', 'port']
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required DB_CONFIG parameter: {key}")
        if not config[key]:
            raise ValueError(f"DB_CONFIG parameter '{key}' cannot be empty")
    try:
        int(config['port'])
    except ValueError:
        raise ValueError("DB_CONFIG parameter 'port' must be a valid integer")

def setup_database():
    try:
        validate_db_config(DB_CONFIG)
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS uav_types (
                id SERIAL PRIMARY KEY,
                type_name VARCHAR(100) UNIQUE NOT NULL
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS regions (
                id SERIAL PRIMARY KEY,
                region_name VARCHAR(100) UNIQUE NOT NULL
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flights (
                id SERIAL PRIMARY KEY,
                flight_id VARCHAR(50) NOT NULL,
                uav_type_id INTEGER REFERENCES uav_types(id),
                departure_point GEOMETRY(POINT, 4326),
                arrival_point GEOMETRY(POINT, 4326),
                departure_time TIMESTAMP NOT NULL,
                arrival_time TIMESTAMP NOT NULL,
                duration_minutes FLOAT,
                region_id INTEGER REFERENCES regions(id),
                source_file VARCHAR(255),
                row_index INTEGER,
                parse_date TIMESTAMP
            );
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_flights_departure_time ON flights(departure_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_flights_flight_id ON flights(flight_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_flights_departure_point ON flights USING GIST(departure_point);")
        
        # Check if the unique constraint exists before adding it
        cur.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 
                    FROM pg_constraint 
                    WHERE conname = 'unique_flight' 
                    AND contype = 'u'
                ) THEN
                    ALTER TABLE flights ADD CONSTRAINT unique_flight UNIQUE (flight_id, departure_time);
                END IF;
            END $$;
        """)
        
        conn.commit()
        cur.close()
        logger.info("База данных успешно настроена")
        return conn
    except Exception as e:
        logger.error(f"Ошибка настройки БД: {e}")
        if conn:
            conn.rollback()
        raise
    
def process_files(input_path: str, conn):
    parser = BPLAParser(conn)
    all_flights = []
    
    if os.path.isdir(input_path):
        files = glob.glob(os.path.join(input_path, '*.xlsx')) + glob.glob(os.path.join(input_path, '*.xls'))
    else:
        files = [input_path]
    
    for file in files:
        try:
            flights = parser.parse_excel_file(file)
            all_flights.extend(flights)
        except Exception as e:
            logger.error(f"Пропущен файл {file} из-за ошибки: {e}")
            parser.error_count += 1
    
    if all_flights and conn:
        parser.insert_to_db(all_flights)
    
    return parser.processed_count, parser.error_count

def main():
    parser = argparse.ArgumentParser(description='Парсер БПЛА с хранением в PostgreSQL/PostGIS')
    parser.add_argument('input_path', help='Путь к .xlsx/.xls файлу или директории')
    parser.add_argument('--no-db', action='store_true', help='Отключить работу с БД для теста')
    
    args = parser.parse_args()
    
    try:
        conn = None
        if not args.no_db:
            conn = setup_database()
        logger.info(f"Обработка {args.input_path}")
        
        processed, errors = process_files(args.input_path, conn)
        
        print(f"\n=== РЕЗУЛЬТАТЫ ===")
        print(f"Обработано: {processed} записей")
        print(f"Ошибок: {errors}")
        print("Логи сохранены в parser.log")
        if not args.no_db:
            print("Данные сохранены в БД PostgreSQL (bpla_db)")
        
        if conn:
            conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Использование: python parser.py <файл.xlsx или директория> [--no-db]")
        print("Пример: python parser.py \"D:\\BPLA\\src\\parsers\\\"")
        sys.exit(1)
    
    sys.exit(main())