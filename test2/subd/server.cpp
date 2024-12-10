#include <iostream>
#include <string>
#include <unistd.h>
#include "server.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <thread>
#include <cstring>
#include <cstdio>

using namespace std;

#define DEFAULT_PORT 7432

void handle_client(int client_socket) {
    try {
        char buffer[4096];
        int bytes_received;

        while ((bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0)) > 0) {
            buffer[bytes_received] = '\0';
            std::string sql_query(buffer, bytes_received);
            sql_query.erase(sql_query.find_last_not_of("\r\n") + 1);

            // Передача sql_query в основной обработчик (например, через вызов функции в main.cpp)
            std::string response = process_query(sql_query); // process_query в main.cpp

            // Отправляем ответ клиенту
            if (send(client_socket, response.c_str(), response.length(), 0) < 0) {
                std::cerr << "SERVER ERROR: Ошибка отправки ответа.\n";
            }
        }
        close(client_socket);

    } catch (const std::exception &e) {
        std::cerr << "SERVER ERROR: Exception in client handler: " << e.what() << '\n';
    }
}

void start_server() {
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        std::cerr << "SERVER ERROR: Ошибка создания сокета.\n";
        return;
    }
    sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(DEFAULT_PORT);
    server_addr.sin_addr.s_addr = INADDR_ANY;


    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "SERVER ERROR: Ошибка привязки сокета.\n";
        close(server_socket);
        return;
    }

    if (listen(server_socket, 5) < 0) {
        std::cerr << "SERVER ERROR: Ошибка прослушивания сокета: " << strerror(errno) << ".\n";
        close(server_socket);
        return;
    } else {
        std::cout << "Listen returned successfully, but errno is: " << strerror(errno) << std::endl;
    }

    while (true) {
        int client_socket = accept(server_socket, nullptr, nullptr);

        if (client_socket < 0) {
            std::cerr << "SERVER ERROR: Ошибка принятия соединения.\n";
            continue;
        }

        // Создаем новый поток для обработки подключения клиента
        std::thread client_thread(handle_client, client_socket);

        client_thread.detach(); // detach вместо join прости ева я не понял зачем он нужен
    }

}