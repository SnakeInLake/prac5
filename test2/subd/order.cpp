#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include "json.hpp" // Используем библиотеку JSON для работы с JSON файлами
#include "DualLinkedList.h"
#include "sql_parser.h"

using namespace std;
using json = nlohmann::json;

namespace fs = filesystem;


// Функция для поиска user_id по ключу пользователя
int findUserIdByKey(const string& user_key, const string& user_table_path) {
    ifstream user_file(user_table_path);
    if (!user_file.is_open()) {
        cerr << "Ошибка: не удалось открыть таблицу пользователей." << endl;
        return -1;
    }

    string line;
    int user_id = -1;

    // Пропускаем заголовок
    if (getline(user_file, line)) {
        while (getline(user_file, line)) {
            istringstream ss(line);
            string id_str, username, key;
            if (getline(ss, id_str, ',') && getline(ss, username, ',') && getline(ss, key, ',')) {
                if (key == user_key) {
                    try {
                        user_id = stoi(id_str);
                    } catch (const std::invalid_argument& e) {
                        cerr << "Ошибка: неверный формат user_id в строке: " << line << endl;
                        return -1;
                    } catch (const std::out_of_range& e) {
                        cerr << "Ошибка: значение user_id выходит за пределы диапазона: " << line << endl;
                        return -1;
                    }
                    break;
                }
            }
        }
    }
    user_file.close();

    return user_id;
}

// Обработка команды (чтение, обновление ордеров, генерация INSERT)
bool processCommand(const string& command) {
    stringstream ss(command);
    string post, path, user_key, params;
    string pair_id_str, quantity_str, price_str, type, clean_type, clean_quantity;

    getline(ss, post, ' '); // POST
    getline(ss, path, ' '); // /order
    getline(ss, params);    // Остальная часть команды

    // Убираем пробелы
    params.erase(0, params.find_first_not_of(' '));
    params.erase(params.find_last_not_of(' ') + 1);

    stringstream params_ss(params);
    getline(params_ss, user_key, ' ');
    getline(params_ss, pair_id_str, ',');
    getline(params_ss, clean_quantity, ',');
    getline(params_ss, price_str, ',');
    getline(params_ss, clean_type, ',');

    string user_table_path = schema_data["name"].get<string>() + "/user/1.csv";

    int user_id = findUserIdByKey(user_key, user_table_path);

    cout<<"user_id "<<user_id<<endl;
    cout<<"user_key "<<user_key<<endl;

    
    for (char c : clean_type) {
        if (c != ' ') {
            type += c;
        }
    }
    for (char c : clean_quantity) {
        if (c != ' ') {
            quantity_str += c;
        }
    }

    int pair_id, quantity;
    double price;
    cout<<"pair_id "<<pair_id_str<<endl;
    cout<<"quantity "<<quantity_str<<endl;

    try {
        pair_id = stoi(pair_id_str);
        quantity = stoi(quantity_str);
        price = stod(price_str);
    } catch (const invalid_argument& e) {
        cerr << "Ошибка: неверный формат данных в команде." << endl;
        return false;
    }

     // === Получение данных пары ===
    string pair_table_path = schema_data["name"].get<string>() + "/pair/1.csv";
    ifstream pair_file(pair_table_path);
    if (!pair_file.is_open()) {
        cerr << "Ошибка: не удалось открыть таблицу пар." << endl;
        return false;
    }

    int first_lot_id = -1, second_lot_id = -1;
    string line;

    if (getline(pair_file, line)) { // Пропускаем заголовок
        while (getline(pair_file, line)) {
            istringstream ss(line);
            string pair_id_str, lot1_str, lot2_str;
            if (getline(ss, pair_id_str, ',') &&
                getline(ss, lot1_str, ',') &&
                getline(ss, lot2_str, ',')) {
                if (stoi(pair_id_str) == pair_id) {
                    first_lot_id = stoi(lot1_str);
                    cout << "first_lot_id " << first_lot_id <<endl;
                    second_lot_id = stoi(lot2_str);
                    cout << "second_lot_id " << second_lot_id <<endl;
                    break;
                }
            }
        }
    }
    pair_file.close();

    if (first_lot_id == -1 || second_lot_id == -1) {
        cerr << "Ошибка: не удалось найти лоты для пары с ID " << pair_id << "." << endl;
        return false;
    }

    // === Списание баланса ===
    string user_lot_table_path = schema_data["name"].get<string>() + "/user_lot/1.csv";
    ifstream user_lot_file(user_lot_table_path);
    if (!user_lot_file.is_open()) {
        cerr << "Ошибка: не удалось открыть таблицу активов пользователей." << endl;
        return false;
    }

    string updated_content;
    bool updated_first_lot = false, updated_second_lot = false;

    string locked_file_path = schema_data["name"].get<string>() + "/user_lot/locked";

    if (getline(user_lot_file, line)) { // Читаем заголовок
        updated_content += line + "\n"; // Сохраняем заголовок
    }

    while (getline(user_lot_file, line)) {
        istringstream ss(line);
        string user_id_str, lot_id_str, quantity_str;
        if (getline(ss, user_id_str, ',') &&
            getline(ss, lot_id_str, ',') &&
            getline(ss, quantity_str, ',')) {
        
            int current_user_id = stoi(user_id_str);
            int current_lot_id = stoi(lot_id_str);
            double current_quantity = stod(quantity_str);

            if (current_user_id == user_id) {
                if (type == "buy" && current_lot_id == second_lot_id) {
                    double cost = quantity * price;
                    if (current_quantity < cost) {
                        cerr << "Ошибка: недостаточно средств для покупки." << endl;
                        return false;
                    }
                    current_quantity -= cost;
                    updated_second_lot = true;
                } else if (type == "sell" && current_lot_id == first_lot_id) {
                    if (current_quantity < quantity) {
                        cerr << "Ошибка: недостаточно средств для продажи." << endl;
                        return false;
                    }
                    current_quantity -= quantity;
                    updated_first_lot = true;
                }
            }

            updated_content += to_string(current_user_id) + "," +
                               to_string(current_lot_id) + "," +
                               to_string(current_quantity) + "\n";
        }
    }
    user_lot_file.close();

    // Проверка: списания должны быть завершены
    if ((type == "buy" && !updated_second_lot) || (type == "sell" && !updated_first_lot)) {
        cerr << "Ошибка: списание завершилось некорректно." << endl;
        return false;
    }
    // Проверяем блокировку таблицы
    ifstream lock_file(locked_file_path);
    if (!lock_file.is_open()) {
        cerr << "Ошибка: не удалось открыть файл блокировки: " << locked_file_path << endl;
        return false;
    }
    string lock_status;
    lock_file >> lock_status;
    lock_file.close();

    if (lock_status == "1") {
        cerr << "Ошибка: таблица " << user_lot_table_path << " заблокирована." << endl;
        return false;
    }

    // Устанавливаем блокировку
    ofstream lock_file_write(locked_file_path);
    lock_file_write << "1" << endl;
    lock_file_write.close();

    // Перезапись файла с обновлениями
    ofstream user_lot_file_write(user_lot_table_path, ios::trunc);
    if (!user_lot_file_write.is_open()) {
        cerr << "Ошибка: не удалось открыть таблицу активов для записи." << endl;
        return false;
    }
    user_lot_file_write << updated_content;
    user_lot_file_write.close();

    cout << "Баланс пользователя успешно обновлен.\n";

    // Снимаем блокировку
    ofstream unlock_file(locked_file_path);
    unlock_file << "0" << endl;
    unlock_file.close();

    string order_table_path = schema_data["name"].get<string>() + "/order/1.csv";

    string locked_file_path2 = schema_data["name"].get<string>() + "/order/locked";

    // Логика обновления ордеров и обработки баланса
    // Путь к файлу ордеров
    string order_table_path_csv = schema_data["name"].get<string>() + "/order/1.csv";
    ifstream order_file(order_table_path_csv);
    vector<string> updated_orders;
    int order_id = 0;

    // Читаем существующие ордера
    if (order_file.is_open()) {
        string line;
        if (getline(order_file, line)) {
            updated_orders.push_back(line); // Заголовок
            while (getline(order_file, line)) {
                updated_orders.push_back(line);
                ++order_id;
            }
        }
        order_file.close();
    } else {
        cerr << "Ошибка: не удалось открыть файл ордеров." << endl;
        return false;
    }

    ++order_id; // Новый order_id

    int matched_quantity = 0;
    double total_cost = 0.0;
    int transaction_id = rand();
    double fulfill_quantity;

    // Обрабатываем противоположные ордера
    for (size_t i = 1; i < updated_orders.size(); ++i) {
        istringstream ss(updated_orders[i]);
        string existing_order_id, existing_user_id, existing_pair_id, existing_quantity, existing_price, existing_type, existing_closed;

        if (getline(ss, existing_order_id, ',') &&
            getline(ss, existing_user_id, ',') &&
            getline(ss, existing_pair_id, ',') &&
            getline(ss, existing_quantity, ',') &&
            getline(ss, existing_price, ',') &&
            getline(ss, existing_type, ',') &&
            getline(ss, existing_closed, ',')) {
            if (existing_closed != "0") continue;

            int existing_qty = stoi(existing_quantity);
            double existing_prc = stod(existing_price);
            if (stoi(existing_pair_id) == pair_id &&
                ((type == "buy" && existing_type == "sell" && price >= existing_prc) ||
                 (type == "sell" && existing_type == "buy" && price <= existing_prc))) {

                fulfill_quantity = min(quantity, existing_qty);
                matched_quantity += fulfill_quantity;
                total_cost += fulfill_quantity * existing_prc;

                // === Начисления балансов ===
                string user_lot_table_path = schema_data["name"].get<string>() + "/user_lot/1.csv";
                ifstream user_lot_file(user_lot_table_path);
                if (!user_lot_file.is_open()) {
                    cerr << "Ошибка: не удалось открыть таблицу активов пользователей." << endl;
                        // Снимаем блокировку
                        ofstream unlock_file(locked_file_path);
                        unlock_file << "0" << endl;
                        unlock_file.close();
                    return false;
                }

                string updated_content;
                bool buyer_updated = false, seller_updated = false;

                // Читаем заголовок
                if (getline(user_lot_file, line)) {
                    updated_content += line + "\n";
                }

                while (getline(user_lot_file, line)) {
                    istringstream user_lot_ss(line);
                    string lot_user_id, lot_id, lot_quantity;

                    if (getline(user_lot_ss, lot_user_id, ',') &&
                        getline(user_lot_ss, lot_id, ',') &&
                        getline(user_lot_ss, lot_quantity, ',')) {

                        int current_user_id = stoi(lot_user_id);
                        int current_lot_id = stoi(lot_id);
                        double current_quantity = stod(lot_quantity);

                        // Обновление баланса покупателя
                        if (current_user_id == user_id) {
                            if (type == "buy" && current_lot_id == first_lot_id) {
                                current_quantity += fulfill_quantity; // Покупатель получает товар
                                buyer_updated = true;
                            } else if (type == "sell" && current_lot_id == second_lot_id) {
                                current_quantity += fulfill_quantity * price; // Покупатель получает валюту
                                buyer_updated = true;
                            }
                        }

                        // Обновление баланса продавца
                        if (current_user_id == stoi(existing_user_id)) {
                            if (existing_type == "sell" && current_lot_id == second_lot_id) {
                                current_quantity += fulfill_quantity * price; // Продавец получает валюту
                                seller_updated = true;
                            } else if (existing_type == "buy" && current_lot_id == first_lot_id) {
                                current_quantity += fulfill_quantity; // Продавец получает товар
                                seller_updated = true;
                            }
                        }

                        updated_content += to_string(current_user_id) + "," + to_string(current_lot_id) + "," + to_string(current_quantity) + "\n";
                    }
                }

                user_lot_file.close();

                // Перезаписываем файл активов
                ofstream user_lot_file_write(user_lot_table_path, ios::trunc);
                if (!user_lot_file_write.is_open()) {
                    cerr << "Ошибка: не удалось открыть таблицу активов для записи." << endl;
                    return false;
                }
                user_lot_file_write << updated_content;
                user_lot_file_write.close();

                cout << "Баланс успешно обновлен: покупатель и продавец.\n";

                // Обновляем существующий ордер
                existing_qty -= fulfill_quantity;
                quantity -= fulfill_quantity;

                updated_orders[i] = existing_order_id + "," + existing_user_id + "," + to_string(pair_id) + "," +
                                    to_string(existing_qty) + "," + to_string(existing_prc) + "," + existing_type + "," +
                                    (existing_qty > 0 ? "0" : to_string(transaction_id));
                if (quantity == 0) break;
            }
        }
    }
        cout<<"ALLGOOD"<<endl;      

         // Проверяем блокировку таблицы
    ifstream lock_file2(locked_file_path2);
    if (!lock_file2.is_open()) {
        cerr << "Ошибка: не удалось открыть файл блокировки: " << locked_file_path2 << endl;
        return false;
    }
    string lock_status2;
    lock_file2 >> lock_status2;
    lock_file2.close();

    if (lock_status2 == "1") {
        cerr << "Ошибка: таблица " << order_table_path << " заблокирована." << endl;
        return false;
    }

    // Устанавливаем блокировку
    ofstream lock_file_write2(locked_file_path2);
    lock_file_write2 << "1" << endl;
    lock_file_write2.close();
    cout<<"ALLGOOD2"<<endl;      

    // Перезаписываем файл ордеров
    ofstream order_file_write(order_table_path_csv, ios::trunc);
    if (!order_file_write.is_open()) {
        cerr << "Ошибка: не удалось открыть файл ордеров для записи." << endl;
        return false;
    }
    for (const auto& line : updated_orders) {
        order_file_write << line << "\n";
    }
    order_file_write.close();

    // Генерация нового ордера
    string closed_value = (quantity == 0) ? to_string(transaction_id) : "0";
    string sql_query_order = "INSERT INTO order VALUES ('" + to_string(order_id) + "', '" +
                             to_string(user_id) + "', '" + to_string(pair_id) + "', '" +
                             to_string(quantity) + "', '" + to_string(price) + "', '" +
                             type + "', '" + closed_value + "')";
 
            // Снимаем блокировку
    ofstream unlock_file2(locked_file_path2);
    unlock_file2 << "0" << endl;
    unlock_file2.close();
    cout<<"ALLGOOD3"<<endl;
    executeInsert(sql_query_order);      
    return 1;
}

bool processDelete(const string& command) {
    stringstream ss(command);
    string post, path, user_key, params;
    string pair_id_str, order_id_str;

    getline(ss, post, ' '); // DELETE
    getline(ss, path, ' '); // /order
    getline(ss, params);    // Остальная часть команды

    // Убираем пробелы
    params.erase(0, params.find_first_not_of(' '));
    params.erase(params.find_last_not_of(' ') + 1);

    stringstream params_ss(params);
    getline(params_ss, user_key, ' ');
    getline(params_ss, order_id_str, ',');

    string user_table_path = schema_data["name"].get<string>() + "/user/1.csv";

    int user_id = findUserIdByKey(user_key, user_table_path);
    if (user_id == -1) {
        cerr << "Ошибка: пользователь с ключом '" << user_key << "' не найден." << endl;
        return false;
    }

    cout << "user_id: " << user_id << ", user_key: " << user_key << ", order_id: " << order_id_str << endl;

    string line, type, pair_id, quantity_str, price_str, closed;

    string order_table_path = "биржа))/order/1.csv";

    ifstream order_file(order_table_path);
    if (!order_file.is_open()) {
        cerr << "Ошибка: не удалось открыть файл ордеров." << endl;
        return false;
    }

    // Находим ордер
    bool order_found = false;
    if (getline(order_file, line)) {
    while (getline(order_file, line)) {
        istringstream ss(line);
        string id_str, user_id_str;

        if (getline(ss, id_str, ',') &&
            getline(ss, user_id_str, ',') &&
            getline(ss, pair_id, ',') &&
            getline(ss, quantity_str, ',') &&
            getline(ss, price_str, ',') &&
            getline(ss, type, ',') &&
            getline(ss, closed, ',')) {
                cout<< "current_order_id "<<id_str<<endl;
                cout<< "current_order_id "<<order_id_str<<endl;

            int current_order_id = stoi(id_str);
            if (current_order_id == stoi(order_id_str)) {
                order_found = true;
                break;
            }
        }
    }
    }
    order_file.close();
    if (closed != "0"){
        cout<<"Ошибка: нельзя удалить закрытый ордер"<<endl;
        return false;
    }
    if (!order_found) {
        cerr << "Ошибка: ордер с ID " << order_id_str << " не найден." << endl;
        return false;
    }

    cout << "Тип ордера: " << type << ", пара: " << pair_id << ", количество: " << quantity_str << ", цена: " << price_str << endl;

    // Поиск лотов для пары
    int first_lot_id = -1, second_lot_id = -1;
    string pair_table_path = "биржа))/pair/1.csv";
    ifstream pair_file(pair_table_path);
    if (!pair_file.is_open()) {
        cerr << "Ошибка: не удалось открыть таблицу пар." << endl;
        return false;
    }

    bool pair_found = false;
    while (getline(pair_file, line)) {
        istringstream ss(line);
        string pair_id_str, lot1_str, lot2_str;

        if (getline(ss, pair_id_str, ',') &&
            getline(ss, lot1_str, ',') &&
            getline(ss, lot2_str, ',')) {

            if (pair_id_str == pair_id) {
                first_lot_id = stoi(lot1_str);
                second_lot_id = stoi(lot2_str);
                pair_found = true;
                break;
            }
        }
    }
    pair_file.close();

    if (!pair_found || first_lot_id == -1 || second_lot_id == -1) {
        cerr << "Ошибка: пара с ID " << pair_id << " не найдена." << endl;
        return false;
    }

    cout << "Лоты для пары: " << first_lot_id << " (первый), " << second_lot_id << " (второй)" << endl;

    // Вычисление суммы возврата
    double refund_amount = 0.0;
    int lot_id_to_update;
    double quantity = stod(quantity_str);
    double price = stod(price_str);
    
    if (type == "buy") {
        lot_id_to_update = second_lot_id;  // Деньги
        refund_amount = quantity * price;
    } else if (type == "sell") {
        lot_id_to_update = first_lot_id;  // Товар
        refund_amount = quantity;
    } else {
        cerr << "Ошибка: неизвестный тип ордера '" << type << "'." << endl;
        return false;
    }

    cout << "Возврат: " << refund_amount << ", lot_id: " << lot_id_to_update << endl;
    
    string locked_file_path = schema_data["name"].get<string>() + "/user_lot/locked";
     // Проверяем блокировку таблицы
    ifstream lock_file(locked_file_path);
    if (!lock_file.is_open()) {
        cerr << "Ошибка: не удалось открыть файл блокировки: " << locked_file_path << endl;
        return false;
    }
    string lock_status;
    lock_file >> lock_status;
    lock_file.close();
    string user_lot_table_path = schema_data["name"].get<string>() + "/user_lot/1.csv";

    if (lock_status == "1") {
        cerr << "Ошибка: таблица " << user_lot_table_path << " заблокирована." << endl;
        return false;
    }

    // Устанавливаем блокировку
    ofstream lock_file_write(locked_file_path);
    lock_file_write << "1" << endl;
    lock_file_write.close();

    // Обновление таблицы user_lot

    ifstream user_lot_file(user_lot_table_path);
    if (!user_lot_file.is_open()) {
        cerr << "Ошибка: не удалось открыть таблицу user_lot." << endl;
        return false;
    }

    string temp_user_lot_path = user_lot_table_path + ".tmp";
    ofstream temp_user_lot_file(temp_user_lot_path);
    if (!temp_user_lot_file.is_open()) {
        cerr << "Ошибка: не удалось создать временный файл для user_lot." << endl;
        return false;
    }

    bool updated = false;
    if (getline(user_lot_file, line)) {
        temp_user_lot_file << line << "\n"; // Копируем заголовок
    }

    while (getline(user_lot_file, line)) {
        istringstream ss(line);
        string user_id_str, lot_id_str, lot_quantity;

        if (getline(ss, user_id_str, ',') &&
            getline(ss, lot_id_str, ',') &&
            getline(ss, lot_quantity, ',')) {
            if (stoi(user_id_str) == user_id && stoi(lot_id_str) == lot_id_to_update) {
                double current_quantity = stod(lot_quantity);
                current_quantity += refund_amount; // Возвращаем средства
                temp_user_lot_file << user_id_str << "," << lot_id_str << "," << current_quantity << "\n";
                updated = true;
            } else {
                temp_user_lot_file << line << "\n";
            }
        }
    }

    if (!updated) {
        temp_user_lot_file << user_id << "," << lot_id_to_update << "," << refund_amount << "\n";
    }

    user_lot_file.close();
    temp_user_lot_file.close();

    if (remove(user_lot_table_path.c_str()) != 0 || rename(temp_user_lot_path.c_str(), user_lot_table_path.c_str()) != 0) {
        cerr << "Ошибка: не удалось обновить файл user_lot." << endl;
        return false;
    }

    cout << "Таблица user_lot успешно обновлена.\n";

    // Снимаем блокировку
    ofstream unlock_file(locked_file_path);
    unlock_file << "0" << endl;
    unlock_file.close();

    // Удаление ордера
    string sql_query_order = "DELETE FROM order WHERE order_id = '" + order_id_str + "'";

    string locked_file_path2 = schema_data["name"].get<string>() + "/order/locked";
     // Проверяем блокировку таблицы
    ifstream lock_file2(locked_file_path2);
    if (!lock_file2.is_open()) {
        cerr << "Ошибка: не удалось открыть файл блокировки: " << locked_file_path2 << endl;
        return false;
    }
    string lock_status2;
    lock_file2 >> lock_status2;
    lock_file2.close();

    if (lock_status2 == "1") {
        cerr << "Ошибка: таблица " << user_lot_table_path << " заблокирована." << endl;
        return false;
    }

    // Устанавливаем блокировку
    ofstream lock_file_write2(locked_file_path2);
    lock_file_write2 << "1" << endl;
    lock_file_write2.close();

    if (!executeDelete(sql_query_order)) {
        cerr << "Ошибка: не удалось удалить ордер с ID " << order_id_str << "." << endl;
            // Снимаем блокировку
        ofstream unlock_file2(locked_file_path2);
        unlock_file2 << "0" << endl;
        unlock_file2.close();
        return false;
    }

    cout << "Ордер успешно удалён.\n";
        // Снимаем блокировку
    ofstream unlock_file2(locked_file_path2);
    unlock_file2 << "0" << endl;
    unlock_file2.close();
    return true;
}

bool processSelect(const string& command){
    stringstream ss(command);
    cout<<"aboba "<<command<<endl;
    string get, clean_path, path, user_key, params;
    string pair_id_str, order_id_str;

    getline(ss, get, ' '); // GET
    getline(ss, clean_path, ' '); // path
    getline(ss, params);    // Остальная часть команды
    for (char c : clean_path){
        if (c != '/'){
            path+=c;
        }
    }
    cout<< "path "<<path<<endl;
    cout<< "params "<<params<<endl;

    // Убираем пробелы
    params.erase(0, params.find_first_not_of(' '));
    params.erase(params.find_last_not_of(' ') + 1);

    stringstream params_ss(params);
    getline(params_ss, user_key, ' ');
    cout<< "user_key "<<user_key<<endl;
    string user_table_path = schema_data["name"].get<string>() + "/user/1.csv";

    int user_id = findUserIdByKey (user_key, user_table_path);

    string user_id_str = to_string(user_id);

    if (user_key == " " or user_key == ""){
        string sql_query_get = "SELECT * FROM " + path;
        executeSelect(sql_query_get);
        return true;
    } else {
        string sql_query_get = "SELECT user_lot.lot_id, user_lot.quantity FROM user_lot WHERE user_lot.user_id=user_lot.user_id AND user_lot.user_id=" + 
        user_id_str;
        cout<< "sql_query_get "<<sql_query_get<<endl;
        executeSelect(sql_query_get);
        return true;

    }
}
