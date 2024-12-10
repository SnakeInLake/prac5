#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <random>
#include <algorithm>
#include "sql_parser.h"

using namespace std;

// Генерация случайного ключа
string generateRandomKey(size_t length = 16) {
    const string charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> distrib(0, charset.size() - 1);

    string key;
    do {
        key.clear();
        for (size_t i = 0; i < length; ++i) {
            key += charset[distrib(gen)];
        }

        // Проверяем на NULL, null и другие нежелательные комбинации
        if (key.find("NULL") != string::npos || key.find("null") != string::npos) {
            key.clear(); // Если есть NULL/null, генерируем заново
        }
    } while (key.empty()); // Повторяем, пока не сгенерируем допустимый ключ

    return key;
}

// Основная функция для генерации SQL-запросов и добавления нового пользователя с балансом
string generateInsertSQLUser(const string& command) {
    // Извлекаем таблицу и данные из команды
    string pk_sequence_filename = "биржа))/user/user_pk_sequence";
    string lot_filename = "биржа))/lot/1.csv";

    stringstream ss(command);
    string post, table_name, username, space;

    size_t pos = command.find(" ");  // Ищем пробел после POST
    if (pos == string::npos) return 0;  // Неверная команда
    table_name = command.substr(pos + 2, command.find(" ", pos + 2) - pos - 1);  // Название таблицы
    username = command.substr(command.find(" ", pos + 1) + 1);  // Имя пользователя

    // Убираем лишние пробелы (если есть)
    table_name.erase(remove(table_name.begin(), table_name.end(), ' '), table_name.end());
    username.erase(remove(username.begin(), username.end(), ' '), username.end());

    cout << "table_name: " << table_name << " username: " << username << endl;

    // Проверка на существование пользователя с таким именем
    ifstream user_file("биржа))/user/1.csv");
    string line;
    bool user_exists = false;
    while (getline(user_file, line)) {
        if (line.find(username) != string::npos) {
            user_exists = true;
            break;
        }
    }
    user_file.close();

    if (user_exists) {
        cout << "Ошибка: пользователь с таким именем уже существует!" << endl;
        return "Ошибка: пользователь с таким именем уже существует!!!";
    }

    // Чтение последнего user_id из файла и инкремент
    int user_id = 0;
    ifstream pk_file(pk_sequence_filename);
    if (pk_file.is_open()) {
        pk_file >> user_id;
        pk_file.close();
    }
    user_id++;  // Генерация нового user_id

    // Записываем новый user_id в файл
    ofstream pk_file_out(pk_sequence_filename);
    if (pk_file_out.is_open()) {
        pk_file_out << user_id;
        pk_file_out.close();
    }

    // Генерация случайного ключа
    string random_key = generateRandomKey();

    // Формируем SQL-запрос для добавления пользователя в таблицу user
    string sql_query_user = "INSERT INTO " + table_name + " VALUES ('" + to_string(user_id) + "', '" 
                            + username + "', '" + random_key + "')";
    // Открываем файл с лотами для чтения
    ifstream lot_file(lot_filename);
    if (!lot_file.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << lot_filename << " для чтения." << endl;
        return "Ошибка: не удалось открыть файл " + lot_filename + " для чтения.";
    }

    // Для каждого лота добавляем запись в таблицу user_lot
    if (getline(lot_file,line)){
    while (getline(lot_file, line)) {
        istringstream ss(line);
        string lot_id, lot_name;

        // Разделяем строку (формат: lot_id,lot_name)
        if (getline(ss, lot_id, ',') && getline(ss, lot_name, ',')) {
            // Формируем SQL-запрос для добавления баланса пользователя в user_lot
            string sql_query_lot = "INSERT INTO user_lot VALUES ('" + to_string(user_id) + "', '" 
                                   + lot_id + "', '1000.0000');";
                string locked_file_path2 = schema_data["name"].get<string>() + "/user_lot/locked";
                // Проверяем блокировку таблицы
                ifstream lock_file(locked_file_path2);
                if (!lock_file.is_open()) {
                    cerr << "Ошибка: не удалось открыть файл блокировки: " << locked_file_path2 << endl;
                    break;
                }
                string lock_status;
                lock_file >> lock_status;
                lock_file.close();
                string user_lot_table_path = schema_data["name"].get<string>() + "/user/1.csv";

                if (lock_status == "1") {
                    cerr << "Ошибка: таблица " << user_lot_table_path << " заблокирована." << endl;
                    break;
                }

                // Устанавливаем блокировку
                ofstream lock_file_write(locked_file_path2);
                lock_file_write << "1" << endl;
                lock_file_write.close();
            // Отправляем запрос в executeInsert
            executeInsert(sql_query_lot);
            // Снимаем блокировку
            ofstream unlock_file(locked_file_path2);
            unlock_file << "0" << endl;
            unlock_file.close();
        }
    }
    }
    lot_file.close();
    cout << "Баланс пользователя (user_id=" << user_id << ") успешно добавлен в таблицу user_lot." << endl;
    return sql_query_user;
}
