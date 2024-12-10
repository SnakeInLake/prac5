#include <iostream>
#include <string>
#include <sstream>
#include <cstring>
#include <regex>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>   
#include <chrono>  
#include <atomic>   
#include <fstream>
#include <vector>

using namespace std;

#define SERVER_PORT 7432
#define SERVER_ADDRESS "127.0.0.1"

namespace fs = std::filesystem;

std::atomic<bool> stop_orders{false}; // Флаг для остановки потока

// Функция подключения к серверу и отправки запроса
std::string send_request(const std::string &request) {
    int sock;
    sockaddr_in server_addr;
    char buffer[4096];
    std::string response;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "CLIENT ERROR: Ошибка создания сокета." << std::endl;
        return "";
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_ADDRESS, &server_addr.sin_addr) <= 0) {
        std::cerr << "CLIENT ERROR: Неверный адрес сервера." << std::endl;
        close(sock);
        return "";
    }

    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "CLIENT ERROR: Ошибка подключения к серверу." << std::endl;
        close(sock);
        return "";
    }

    if (send(sock, request.c_str(), request.length(), 0) < 0) {
        std::cerr << "CLIENT ERROR: Ошибка отправки запроса." << std::endl;
        close(sock);
        return "";
    }

    int bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received > 0) {
        buffer[bytes_received] = '\0';
        response = std::string(buffer);
    } else {
        std::cerr << "CLIENT ERROR: Ошибка получения ответа." << std::endl;
    }

    close(sock);
    return response;
}

// Функция обработки ответа и получения ключа
std::string extract_key(const std::string &response) {
    std::regex key_regex(R"(Запись значения: ([a-zA-Z0-9]{10,}))");
    std::smatch match;

    if (std::regex_search(response, match, key_regex) && match.size() > 1) {
        return match[1].str();
    }
    return "";
}

int connect_to_server() {
    int sock;
    sockaddr_in server_addr;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "CLIENT ERROR: Ошибка создания сокета." << std::endl;
        return -1;
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_ADDRESS, &server_addr.sin_addr) <= 0) {
        std::cerr << "CLIENT ERROR: Неверный адрес сервера." << std::endl;
        close(sock);
        return -1;
    }

    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "CLIENT ERROR: Ошибка подключения к серверу." << std::endl;
        close(sock);
        return -1;
    }

    return sock;
}

void process_orders(const std::string &file_path, const std::string &key) {
    int socket_fd = connect_to_server();

    while (true) {
        std::ifstream file(file_path);
        if (!file.is_open()) {
            std::cerr << "Ошибка: не удалось открыть файл " << file_path << std::endl;
            return;
        }

        std::string line;
        std::getline(file, line); // Пропускаем заголовок

        while (std::getline(file, line)) {
            std::istringstream line_stream(line);
            std::string value;
            std::vector<std::string> row;

            while (std::getline(line_stream, value, ',')) {
                row.push_back(value);
            }

            // Проверяем, что у нас корректное количество столбцов
            if (row.size() < 7) {
                std::cerr << "Ошибка: строка содержит недостаточно столбцов: " << line << std::endl;
                continue;
            }

            // Извлекаем нужные значения
            int order_id = stoi(row[0]);
            int pair_id = std::stoi(row[2]);
            double quantity = std::stod(row[3]);
            double price = std::stod(row[4]);
            std::string type = row[5];
            int closed = std::stoi(row[6]);
            cout<<"pair_id "<< pair_id<<endl;
            cout<<"price "<< price<<endl;
            cout<<"type "<< type<<endl;
            cout<<"closed "<< closed<<endl;

            // Условие: pair_id = 1, 2, 3, 4; type = buy; closed = 0
            if ((pair_id == 1 || pair_id == 2 || pair_id == 3 || pair_id == 4) && type == "sell" && closed == 0) {
                    // Формируем запрос
                std::ostringstream request_stream;
                request_stream << "POST /order "<< key <<" "<<pair_id
                            << "," << quantity
                            << "," << price
                            << ",buy";
                std::string request = request_stream.str();
                std::cout << "request " << request<<std::endl;

                // Отправляем запрос
                if (send(socket_fd, request.c_str(), request.length(), 0) < 0) {
                    std::cerr << "Ошибка отправки запроса на сервер.\n";
                    return;
                }

                // Получаем ответ от сервера
                char buffer[4096];
                int bytes_received = recv(socket_fd, buffer, sizeof(buffer) - 1, 0);
                if (bytes_received <= 0) {
                    std::cerr << "Ошибка получения ответа от сервера.\n";
                    return;
                }

                buffer[bytes_received] = '\0';
                std::string response(buffer);

                // Проверяем ответ
                if (response.find("Ордер успешно создан") != std::string::npos) {
                    std::cout << "Успех: " << response << "\n";
                } else {
                    std::cerr << "Ошибка: " << response << "\n";
                }
                std::cout << "Обработан ордер ID: " << order_id << " (pair_id: " << pair_id << ", price: " << price << ", quantity: " << quantity << ")\n";
            }
        }

        file.close();
        std::this_thread::sleep_for(std::chrono::seconds(1)); // Ожидание перед следующей проверкой
    }
}


int main() {
    std::string key;
    int socket_fd = connect_to_server();

    while (true) {
        std::cout << "Выберите действие:\n";
        std::cout << "1. Зарегистрировать пользователя\n";
        std::cout << "2. Запустить автоматическое выставление ордеров\n";
        std::cout << "3. Остановить автоматическое выставление ордеров\n";
        std::cout << "0. Выход\n";
        std::cout << "Ваш выбор: ";
        
        int choice;
        std::cin >> choice;

        if (choice == 0) {
            std::cout << "Завершение программы.\n";
            stop_orders = true;
            break;
        } else if (choice == 1) {
            std::string request = "POST /user TestBot\n";
            std::cout << "Отправка запроса на сервер...\n";
            std::string response = send_request(request);

            if (!response.empty()) {
                std::cout << "Ответ сервера:\n" << response << std::endl;
                key = extract_key(response);

                if (!key.empty()) {
                    std::cout << "Успешная регистрация! Ваш ключ: " << key << std::endl;
                } else {
                    std::cout << "Не удалось извлечь ключ. Введите ключ вручную: ";
                    std::cin >> key;
                }
            } else {
                std::cerr << "Ошибка: Пустой ответ от сервера.\n";
            }
        } else if (choice == 2) {
            if (key.empty()) {
                std::cerr << "Ошибка: Сначала зарегистрируйтесь и получите ключ (нажмите 1).\n";
            } else {
                process_orders("биржа))/order/1.csv", key);
            }
        } else if (choice == 3) {
            stop_orders = true;
        } else {
            std::cerr << "Ошибка: Неверный выбор. Повторите попытку.\n";
        }
    }

    return 0;
}
