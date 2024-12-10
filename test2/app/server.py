import os
import socket
from flask import Flask, request
import time

app = Flask(__name__)

# Получаем номер сервера
server_number = os.getenv('SERVER_NUMBER', '1')

# Конфигурация подключения к СУБД
DB_HOST = '172.18.0.2'  # Адрес СУБД
DB_PORT = 7432         # Порт, на котором слушает СУБД

def send_to_db(query):
    time.sleep(1)  # Уменьшили задержку
    print(f"DB_HOST: {DB_HOST}")
    print(f"Sending query to DB: {query}")  # Логируем запрос
    try:
        with socket.create_connection((DB_HOST, DB_PORT)) as db_socket:
            db_socket.sendall(query.encode('utf-8'))
            response = db_socket.recv(8192)
            print(f"Received response from DB: {response.decode('utf-8')}")  # Логируем ответ
            return response.decode('utf-8')
    except Exception as e:
        print(f"Error communicating with DB: {e}")  # Логируем ошибку
        return f"Ошибка взаимодействия с СУБД: {e}"

@app.route('/user', methods=['POST'])
def create_user():
    username = request.get_data(as_text=True)
    query = f"POST /user {username}"
    db_response = send_to_db(query)
    return db_response

@app.route('/order', methods=['POST', 'DELETE', 'GET'])
def order():
    if request.method == 'POST':
        data = request.get_data(as_text=True)
        query = f"POST /order {data}"
    elif request.method == 'DELETE':
        data = request.get_data(as_text=True)
        query = f"DELETE /order {data}"
    else:  # GET
        query = "GET /order"
    db_response = send_to_db(query)
    return db_response

@app.route('/lot', methods=['GET'])
def lot():
    try:
        print("Inside lot() function")  # Проверка
        query = "GET /lot"
        db_response = send_to_db(query)
        print(f"Returning response: {db_response}")
        return db_response
    except Exception as e:
        print(f"Exception in lot() function: {e}")
        return f"Error: {e}"

@app.route('/pair', methods=['GET'])
def pair():
    query = "GET /pair"
    db_response = send_to_db(query)
    return db_response

@app.route('/balance', methods=['GET'])
def balance():
    query_params = []
    for key, value in request.args.items():
        if value:
            query_params.append(f"{key}={value}")
        else:
            query_params.append(key)

    query_string = " ".join(query_params)
    query = f"GET /balance {query_string}"
    db_response = send_to_db(query)
    return db_response

if __name__ == '__main__':
    print("Starting Flask app...")
    app.run(host='0.0.0.0', port=8000)
