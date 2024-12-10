# Используем базовый образ с Python
FROM python:3.10-slim

# Устанавливаем необходимые зависимости
WORKDIR /app

# Копируем файлы приложения в контейнер
COPY . /app

# Устанавливаем Flask
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install requests

# Указываем команду для запуска Python-сервера
CMD ["python3", "server.py"]

EXPOSE 5000
