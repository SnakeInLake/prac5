#include "sql_parser.h"
#include "schema_creator.h"
#include "request_to_sql.h"
#include "order.h"
#include "capture.h"
#include "server.h"
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <exception>

using json = nlohmann::json;

// Функция для обработки SQL-запросов
std::string process_query(const std::string& sql_query) {
    std::cout << "Обработка запроса: " << sql_query << std::endl;  // Отладка
        stringstream ss(sql_query);
        string command;
        string table;
        ss >> command;  // Извлекаем команду из запроса
        cout << "command "<<command<<endl;
        ss >> table;
        cout << "table "<<table<<endl;
        try {
            if (command == "POST") {
                if (table == "/user"){
                // Запрос типа POST (например, создание пользователя)

                OutputCapturer capturer; // Создаем объект для перехвата вывод
                string sql = generateInsertSQLUser(sql_query);  // Преобразуем запрос в SQL

                bool success = executeInsert(sql); // Выполняем DELETE
                std::string post_result = capturer.getCapturedOutput(); // Получаем перехваченный вывод
                std::cout << "Результат POST: " << post_result << std::endl;  // Отладка

                // Выполняем вставку данных
                if (success) {
                    return "POST_RESULT:" + post_result; // Возвращаем результат с префиксом
                } else {
                    return "Ошибка выполнения запроса POST.\n"; 
                }
                
            } else if (table == "/order"){
                OutputCapturer capturer; // Создаем объект для перехвата вывода

                // Запрос типа POST (например, создание ордера)
                bool success = processCommand(sql_query); // Выполняем POST
                std::string post_result = capturer.getCapturedOutput(); // Получаем перехваченный вывод
                std::cout << "Результат POST: " << post_result << std::endl;  // Отладка

                // Выполняем вставку данных
                if (success) {
                    return "POST_RESULT:" + post_result; // Возвращаем результат с префиксом
                } else {
                    return "Ошибка выполнения запроса POST.\n"; 
                }
            } 
            } else if (command == "DELETE"){
                OutputCapturer capturer; // Создаем объект для перехвата вывода
                // Запрос типа DELETE 




                std::string post_result = capturer.getCapturedOutput(); // Получаем перехваченный вывод
                std::cout << "Результат DELETE: " << post_result << std::endl;  // Отладка
                bool success = processDelete(sql_query); // Выполняем POST
                // Выполняем вставку данных
                if (success) {
                    return "POST_RESULT:" + post_result; // Возвращаем результат с префиксом
                } else {
                    return "Ошибка выполнения запроса DELETE.\n"; 
                }
            } else if (command == "GET"){
                processSelect(sql_query);
                OutputCapturer capturer; // Создаем объект для перехвата вывода
                // Запрос типа DELETE 
                bool success = processSelect(sql_query); // Выполняем POST

                std::string post_result = capturer.getCapturedOutput(); // Получаем перехваченный вывод
                std::cout << "Результат GET: " << post_result << std::endl;  // Отладка
                // Выполняем вставку данных
                if (success) {
                    return "POST_RESULT:" + post_result; // Возвращаем результат с префиксом
                } else {
                    return "Ошибка выполнения запроса GET.\n"; 
                }
            }
        } catch (const exception& e) {
            cerr << "Ошибка: " << e.what() << endl;
        }
    return table;
}


int main() {
    std::cout << "Загрузка схемы..." << std::endl;
    if (!loadSchema("schema.json")) {
        std::cerr << "SERVER ERROR: Ошибка загрузки схемы.\n";
        return 1;
    }
    std::cout << "Схема загружена успешно." << std::endl;

    std::string schema_name = schema_data["name"];
    std::cout << "Создание схемы: " << schema_name << std::endl;
    if (!createSchema("schema.json", schema_name)) {
        std::cerr << "SERVER ERROR: Ошибка создания схемы.\n";
        return 1;
    }
    std::cout << "Схема создана успешно." << std::endl;
    
    // Заполняем лоты
    std::cout << "Заполнение таблицы лотов..." << std::endl;
    if (!populateLots(schema_name)) {
        std::cerr << "Ошибка: не удалось заполнить таблицу лотов." << std::endl;
        return 1;
    }
    std::cout << "Таблица лотов заполнена успешно." << std::endl;

    // Создаем пары
    std::cout << "Создание валютных пар..." << std::endl;
    if (!populatePairs(schema_name)) {
        std::cerr << "SERVER ERROR: Ошибка формирования валютных пар.\n";
        return 1;
    }
    std::cout << "Валютные пары созданы успешно." << std::endl;

    // Запуск сервера
    std::cout << "Запуск сервера..." << std::endl;
    start_server();
    cout<<" сервер запущен "<<endl;
    return 0;
}