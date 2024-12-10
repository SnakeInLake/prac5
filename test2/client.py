import requests

SERVER_URL = "http://localhost:8080"

def main():
    print("Введите команду (например, POST /user biba123):")
    while True:
        try:
            # Читаем команду от пользователя
            command = input("> ").strip()
            if not command:
                continue
            method, path, *body = command.split(' ', 2)
            body = body[0] if body else ""

            # Отправляем запрос
            if method.upper() in ["POST", "DELETE"]:
                response = requests.request(method.upper(), f"{SERVER_URL}{path}", data=body)
            elif method.upper() == "GET":
                response = requests.get(f"{SERVER_URL}{path}", params=body)
            else:
                print("Неподдерживаемый метод!")
                continue

            # Печатаем результат
            print(f"Ответ сервера:\n{response.text}")
        except Exception as e:
            print(f"Ошибка: {e}")

if __name__ == "__main__":
    main()
