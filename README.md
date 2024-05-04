# CLI для периодического получения данных о погоде, записи данных в БД и экспорта в .xlsx файл
## Запуск проекта 
- Скрипт поддерживает установку двух опциональных параметров:
  
  -  "--frequency «int»‎" или "-f «int»‎" — частота (в минутах) обновления данных и их вставки в БД
  -  "--excel" — при установке этого флага выполняется экспорт в файл "weather_data.xlsx" рабочей директории
    
- Пример запуска:
````
python script.py --excel --frequency 1
````
## Установка и конфигурация проекта 
- Клонирование репозитория
  
````
git clone https://github.com/kamilgareev/current_weather_cli
````

- Создание виртуального окружения
  
    ````
    python -m venv venv
    ````
- Активация виртуального окружения
  - Windows

    ````
    venv\Scripts\activate
    ````
  - Linux или MacOS
    
    ````
    source venv/bin/activate
    ````
- Установка необходимых зависимостей 
````
pip install -r requirements.txt
````
- Установка параметров базы данных в файле .env
````
DB_HOST=...
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
DB_PORT=...
````
