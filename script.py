import argparse
import asyncio
import os
import sys
from typing import Tuple, Dict, Any, List

import aiohttp
import asyncpg
from dotenv import load_dotenv
from openpyxl.styles import Font
from openpyxl.workbook import Workbook


def get_cli_args() -> Tuple[Any, Any]:
    """
    Получение агрументов из командной строки.
    Возможные значения: -f или --frequency <int> — частота обновления данных и их вставки в БД в минутах;
                        --excel — при установке этого флага выполняется экспорт в .xlsx файл.
    Оба параметра являются опциональными.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--frequency', action='store')

    parser.add_argument('--excel', action='store_true')
    args = parser.parse_args()

    return args.frequency, args.excel


def convert_wind_direction(wind_direction: str) -> str:
    """
    Получение направления ветра в нужном формате.
    """

    match wind_direction:
        case 'nw':
            return 'Cеверо-западное'
        case 'n':
            return 'Северное'
        case 'ne':
            return 'Северно-восточное'
        case 'e':
            return 'Восточное'
        case 'se':
            return 'Юго-восточное'
        case 's':
            return 'Южное'
        case 'sw':
            return 'Юго-западное'
        case 'w':
            return 'Западное'
        case 'c':
            return 'Штиль'


def convert_precipitation_type(precipitation_type: int) -> str:
    """
    Получение вида осадков в нужном формате.
    """

    match precipitation_type:
        case 0:
            return 'Без осадков'
        case 1:
            return 'Дождь'
        case 2:
            return 'Дождь со снегом'
        case 3:
            return 'Снег'
        case 4:
            return 'Град'


def convert_precipitation_strength(precipitation_strength: float) -> str:
    """
    Получение интенсивности осадков в нужном формате.
    """
    match precipitation_strength:
        case 0:
            return 'Без осадков'
        case 0.25:
            return 'Слабые осадки'
        case 0.5:
            return 'Рядовые осадки'
        case 0.75:
            return 'Сильные осадки'
        case 1:
            return 'Очень сильные осадки'


async def parse_weather_data(data: Dict[str, Any]) -> Dict[str, any]:
    """
    Парсинг полученных с сервиса данных.
    """
    temperature = float(data['temp'])
    wind_speed = float(data['wind_speed'])
    pressure = float(data['pressure_mm'])

    loop = asyncio.get_event_loop()

    wind_direction = await loop.run_in_executor(None, convert_wind_direction, data['wind_dir'])
    precipitation_type = await loop.run_in_executor(None, convert_precipitation_type, data['prec_type'])
    precipitation_strength = await loop.run_in_executor(None, convert_precipitation_strength, data['prec_strength'])

    return {
        'temperature': temperature,
        'wind_direction': wind_direction,
        'wind_speed': wind_speed,
        'pressure': pressure,
        'precipitation_type': precipitation_type,
        'precipitation_strength': precipitation_strength
    }


async def async_get_weather_data(latitude: str, longitude: str, api_key: str) -> Dict[str, Any]:
    """
    Получение данных о текущей погоде с сервиса и их парсинг с помощью функции "parse_weather_data".
    """
    url = 'https://api.weather.yandex.ru/v2/forecast'
    params = {
        'lat': latitude,
        'lon': longitude,
    }
    headers = {'X-Yandex-Weather-Key': api_key}

    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url=url, params=params, headers=headers)
        except aiohttp.ClientResponseError as e:
            sys.exit(f'Ошибка на стороне сервиса с данными о погоде. Детали: {e}')
        data = await response.json()

    parsed_data = await parse_weather_data(data['fact'])
    return parsed_data


async def async_get_connection() -> asyncpg.connection.Connection:
    """
    Получение соединения с БД.
    """
    try:
        load_dotenv('./.env')
        connection = await asyncpg.connect(database=os.getenv('DB_NAME'),
                                           user=os.getenv('DB_USER'),
                                           password=os.getenv('DB_PASSWORD'),
                                           host=os.getenv('DB_HOST'),
                                           port=os.getenv('DB_PORT'))
    except Exception as e:
        sys.exit(f'Ошибка в конфигурации данных БД или расположения файла .env. Детали: {e}')
    return connection


async def async_create_table(connection: asyncpg.connection.Connection) -> None:
    """
    Создание таблицы, если ее еще не существует.
    """
    sql_query = """
    CREATE TABLE IF NOT EXISTS weather_data (id serial PRIMARY KEY,\
    temperature REAL, wind_direction varchar(20), wind_speed REAL, pressure REAL,\
    precipitation_type varchar(20), precipitation_strength varchar(25));
    """
    await connection.execute(sql_query)


async def async_insert_data_into_db(connection: asyncpg.connection.Connection, data: Dict[str, Any]) -> None:
    """
    Вставка данных в таблицу.
    """
    values = list(data.values())
    sql_query = "INSERT INTO weather_data (temperature,\
                wind_direction, wind_speed, pressure, precipitation_type,\
                precipitation_strength)\
                VALUES ($1, $2, $3, $4, $5, $6);"
    await connection.execute(sql_query, *values)


async def async_get_records_count(connection: asyncpg.connection.Connection) -> int:
    """
    Получение количества записей.
    """
    sql_query = """
    SELECT count(*) AS exact_count FROM weather_data;
    """
    records = await connection.fetch(sql_query)
    result = [dict(record) for record in records][0].values()
    return int(list(result)[0])


async def async_get_ten_last_records(connection: asyncpg.connection.Connection) -> List[List[Any]]:
    """
    Получение последних десяти записей.
    """
    records_count = await async_get_records_count(connection)
    if records_count < 10:  # если записей менее 10, возвращаем пустой список для последующей проверки
        return []

    sql_query = """
    SELECT temperature, wind_direction, wind_speed, pressure,\
    precipitation_type, precipitation_strength FROM weather_data \
    ORDER BY ID DESC
    LIMIT 10;
    """

    records = await connection.fetch(sql_query)
    result = [dict(record) for record in records]
    return [list(res.values()) for res in result]


async def async_get_data_for_xlsx_file(connection: asyncpg.connection.Connection) -> List[List[Any]]:
    """
    Формирование итоговых данных для экспорта в .xlsx файл.
    """
    headers = ['Температура', 'Направление ветра', 'Скорость ветра', 'Атмосферное давление',
               'Тип осадков', 'Количество осадков']

    records = await async_get_ten_last_records(connection)
    if not records:  # если записей менее 10, возвращаем пустой список для последующей проверки
        return []
    return [headers] + records


def export_data_to_xlsx_file(data: List[List[Any]]) -> None:
    """
    Экспорт в .xlsx файл.
    """
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = 'Данные'

    for row in data:
        worksheet.append(row)

    ft = Font(bold=True)
    for row in worksheet["A1:F1"]:
        for cell in row:
            cell.font = ft

    workbook.save('weather_data.xlsx')


async def main():
    loop = asyncio.get_event_loop()
    # получение частоты обновления и информации об экспорте данных в .xlsx файл
    frequency, export_to_xlsx = await loop.run_in_executor(None, get_cli_args)

    if not frequency and not export_to_xlsx:  # если не указан ни один из флагов, то скрипт не работает
        sys.exit('Для работы скрипта должен быть указан один из флагов: --excel, --frequency <int>')

    connection = await async_get_connection()  # получение соединения с БД

    if export_to_xlsx:  # если указан флаг "--excel", нужно экспортировать данные в .xlsx файл
        data_for_xlsx_file = await async_get_data_for_xlsx_file(connection)  # получение данных для экспорта

        if data_for_xlsx_file:  # если данные получены (если записей в БД не менее 10)
            loop = asyncio.get_event_loop()
            loop.run_in_executor(None, export_data_to_xlsx_file, data_for_xlsx_file)  # экспорт данных
            print('Экспорт данных в файл "weather_data.xlsx" успешно выполнен.')
        else:
            print('Количество записей в БД меньше 10. Экспорт данных не выполнен.')

    if frequency:  # если указан флаг "--frequency <int>", нужно добавлять данные в БД
        latitude = '55.698538'
        longitude = '37.359576'
        api_key = '487492f0-7801-4c1f-8a28-79f65424e32e'

        await async_create_table(connection)  # создание таблицы в БД

        while True:
            weather_data = await async_get_weather_data(latitude, longitude, api_key)  # получение данных
            await async_insert_data_into_db(connection, weather_data)  # добавление данных в БД

            print(f'В БД добавлена новая запись: {weather_data}.')
            await asyncio.sleep(int(frequency) * 60)  # остановка работы скрипта на "frequency" минут


if __name__ == '__main__':
    try:
        asyncio.run(main())  # запуск скрипта
    except KeyboardInterrupt:  # при нажатии "ctrl + c" скрипт останавливается с "красивым" сообщением
        sys.exit('\nРабота скрипта остановлена.')
