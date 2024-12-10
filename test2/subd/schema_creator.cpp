#include "schema_creator.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <string>
#include "json.hpp"

namespace fs = std::filesystem;
using json = nlohmann::json;

bool createSchema(const std::string& schema_filename, const std::string& schema_name) {
    std::ifstream schema_file(schema_filename);
    if (!schema_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл " << schema_filename << std::endl;
        return false;
    }

    json schema_data;
    try {
        schema_file >> schema_data;
    } catch (const json::parse_error& e) {
        std::cerr << "Ошибка парсинга JSON: " << e.what() << std::endl;
        return false;
    }

    if (!fs::exists(schema_name)) {
        std::cout << "Папка схемы отсутствует, создаём: " << schema_name << std::endl;
        fs::create_directory(schema_name);
    }

    for (const auto& [table_name, columns] : schema_data["structure"]["tables"].items()) {
        std::string table_path = schema_name + "/" + table_name;

        if (!fs::exists(table_path)) {
            std::cout << "Папка таблицы отсутствует, создаём: " << table_path << std::endl;
            fs::create_directory(table_path);
        }

        std::string csv_filename = table_path + "/1.csv";
        if (!fs::exists(csv_filename)) {
            std::cout << "Файл 1.csv отсутствует, создаём: " << csv_filename << std::endl;
            std::ofstream csv_file(csv_filename);
            if (!csv_file.is_open()) {
                std::cerr << "Ошибка: не удалось создать CSV файл для " << table_name << std::endl;
                return false;
            }

            // Записываем заголовки столбцов
            for (size_t i = 0; i < columns.size(); ++i) {
                csv_file << columns[i].get<std::string>();
                if (i < columns.size() - 1) {
                    csv_file << ",";
                }
            }
            csv_file << std::endl;
            csv_file.close();
        }

        std::string pk_sequence_filename = table_path + "/" + table_name + "_pk_sequence";
        if (!fs::exists(pk_sequence_filename)) {
            std::cout << "Файл " << table_name << "_pk_sequence отсутствует, создаём: " << pk_sequence_filename << std::endl;
            std::ofstream pk_file(pk_sequence_filename);
            if (!pk_file.is_open()) {
                std::cerr << "Ошибка: не удалось создать файл первичного ключа для " << table_name << std::endl;
                return false;
            }
            pk_file << "0" << std::endl;  // Начальное значение первичного ключа
            pk_file.close();
        }

        std::string locked_filename = table_path + "/locked";
        if (!fs::exists(locked_filename)) {
            std::cout << "Файл locked отсутствует, создаём: " << locked_filename << std::endl;
            std::ofstream locked_file(locked_filename);
            if (!locked_file.is_open()) {
                std::cerr << "Ошибка: не удалось создать файл locked для " << table_name << std::endl;
                return false;
            }
            locked_file << "0" << std::endl;  // Блокировка: 0 — не заблокировано
            locked_file.close();
        }
    }

    std::cout << "Схема успешно создана/проверена." << std::endl;
    return true;
}

// Функция для заполнения таблицы lot из config.json
bool populateLots(const std::string& schema_name) {
    const std::string config_filename = "config.json"; // ЖЁСКА)) заданное имя файла конфигурации
    const std::string table_name = "lot"; // Таблица для записи лотов
    const std::string table_path = schema_name + "/" + table_name;
    const std::string lot_csv_filename = table_path + "/1.csv";
    const std::string pk_sequence_filename = table_path + "/" + table_name + "_pk_sequence";

    // Загружаем config.json
    std::ifstream config_file(config_filename);
    if (!config_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл конфигурации " << config_filename << std::endl;
        return false;
    }

    json config_data;
    try {
        config_file >> config_data;
    } catch (const json::parse_error& e) {
        std::cerr << "Ошибка парсинга JSON конфигурации: " << e.what() << std::endl;
        return false;
    }

    // Проверяем наличие массива "lots" в конфигурации
    if (!config_data.contains("lots") || !config_data["lots"].is_array()) {
        std::cerr << "Ошибка: в файле конфигурации отсутствует массив 'lots'." << std::endl;
        return false;
    }

    // Проверяем существование таблицы lot
    if (!fs::exists(lot_csv_filename)) {
        std::cerr << "Ошибка: таблица lot не существует. Убедитесь, что схема создана." << std::endl;
        return false;
    }

    // Читаем последний lot_id из _pk_sequence
    int lot_id = 0;
    std::ifstream pk_file(pk_sequence_filename);
    if (pk_file.is_open()) {
        pk_file >> lot_id;
        pk_file.close();
    }
    // Проверяем, заполнен ли файл pair/1.csv
if (fs::exists(lot_csv_filename)) {
    std::ifstream pair_file(lot_csv_filename);
    if (!pair_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл " << lot_csv_filename << " для проверки заполненности.\n";
        return false;
    }

    std::string line;
    int line_count = 0;

    // Считаем количество строк в файле
    while (std::getline(pair_file, line)) {
        ++line_count;
    }
    pair_file.close();

    // Если файл содержит больше одной строки (заголовок + хотя бы одна запись), пропускаем заполнение
    if (line_count > 1) {
        std::cout << "Файл " << lot_csv_filename << " уже заполнен. Пропускаем создание лотов.\n";
        return true;
    }
}
    // Открываем CSV для добавления лотов
    std::ofstream csv_file(lot_csv_filename, std::ios::app);
    if (!csv_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл " << lot_csv_filename << " для записи." << std::endl;
        return false;
    }

    // Добавляем лоты в таблицу
    for (const auto& lot : config_data["lots"]) {
        if (!lot.is_string()) {
            std::cerr << "Ошибка: один из элементов 'lots' не является строкой." << std::endl;
            continue; // Пропускаем невалидный элемент
        }

        lot_id++; // Генерируем новый идентификатор
        csv_file << lot_id << "," << lot.get<std::string>() << std::endl;
    }
    csv_file.close();

    // Обновляем _pk_sequence
    std::ofstream pk_file_out(pk_sequence_filename);
    if (!pk_file_out.is_open()) {
        std::cerr << "Ошибка: не удалось обновить _pk_sequence для таблицы lot." << std::endl;
        return false;
    }
    pk_file_out << lot_id << std::endl;
    pk_file_out.close();

    std::cout << "Лоты успешно добавлены в таблицу lot." << std::endl;
    return true;
}
bool populatePairs(const std::string& schema_name) {
    const std::string lot_table_name = "lot"; // Таблица лотов
    const std::string pair_table_name = "pair"; // Таблица пар
    const std::string lot_csv_filename = schema_name + "/" + lot_table_name + "/1.csv";
    const std::string pair_csv_filename = schema_name + "/" + pair_table_name + "/1.csv";
    const std::string pair_pk_sequence_filename = schema_name + "/" + pair_table_name + "_pk_sequence";

    // Проверяем, существует ли таблица lot
    if (!fs::exists(lot_csv_filename)) {
        std::cerr << "Ошибка: таблица lot не существует. Убедитесь, что схема создана." << std::endl;
        return false;
    }

    // Проверяем, существует ли таблица pair
    if (!fs::exists(pair_csv_filename)) {
        std::cerr << "Ошибка: таблица pair не существует. Убедитесь, что схема создана." << std::endl;
        return false;
    }

// Проверяем, заполнен ли файл pair/1.csv
if (fs::exists(pair_csv_filename)) {
    std::ifstream pair_file(pair_csv_filename);
    if (!pair_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл " << pair_csv_filename << " для проверки заполненности.\n";
        return false;
    }

    std::string line;
    int line_count = 0;

    // Считаем количество строк в файле
    while (std::getline(pair_file, line)) {
        ++line_count;
    }
    pair_file.close();

    // Если файл содержит больше одной строки (заголовок + хотя бы одна запись), пропускаем заполнение
    if (line_count > 1) {
        std::cout << "Файл " << pair_csv_filename << " уже заполнен. Пропускаем создание валютных пар.\n";
        return true;
    }
}

    // Считываем все лоты из таблицы lot
    std::ifstream lot_file(lot_csv_filename);
    if (!lot_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл " << lot_csv_filename << " для чтения." << std::endl;
        return false;
    }

    std::vector<std::pair<int, std::string>> lots; // Храним lot_id и имя лота
    std::string line;

    // Пропускаем заголовок
    if (!std::getline(lot_file, line)) {
        std::cerr << "Ошибка: файл " << lot_csv_filename << " пуст или повреждён." << std::endl;
        return false;
    }

    // Считываем все строки
    while (std::getline(lot_file, line)) {
        std::istringstream ss(line);
        std::string lot_id_str, lot_name;

        // Разделяем по запятым (формат: lot_id,lot_name)
        if (std::getline(ss, lot_id_str, ',') && std::getline(ss, lot_name, ',')) {
            int lot_id = std::stoi(lot_id_str); // Преобразуем lot_id в число
            lots.emplace_back(lot_id, lot_name);
        }
    }
    lot_file.close();

    // Проверяем, есть ли хотя бы два лота
    if (lots.size() < 2) {
        std::cerr << "Ошибка: недостаточно лотов для формирования пар." << std::endl;
        return false;
    }

    // Открываем файл для записи пар
    std::ofstream pair_file(pair_csv_filename, std::ios::app);
    if (!pair_file.is_open()) {
        std::cerr << "Ошибка: не удалось открыть файл " << pair_csv_filename << " для записи." << std::endl;
        return false;
    }

    // Читаем последний pair_id из _pk_sequence
    int pair_id = 0;
    std::ifstream pair_pk_file(pair_pk_sequence_filename);
    if (pair_pk_file.is_open()) {
        pair_pk_file >> pair_id;
        pair_pk_file.close();
    }

    // Формируем пары "каждый с каждым" (без повторов)
    for (size_t i = 0; i < lots.size(); ++i) {
        for (size_t j = i + 1; j < lots.size(); ++j) {
            pair_id++; // Увеличиваем идентификатор пары
            pair_file << pair_id << "," << lots[i].first << "," << lots[j].first << std::endl;
        }
    }
    pair_file.close();

    std::cout << "Валютные пары успешно добавлены в таблицу pair." << std::endl;
    return true;
}