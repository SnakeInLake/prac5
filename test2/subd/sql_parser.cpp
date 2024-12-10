#include "sql_parser.h"
#include "DualLinkedList.h"
#include "json.hpp"
#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <set>


using namespace std;

namespace fs = filesystem;

using json = nlohmann::json;

json schema_data;

bool loadSchema(const string& schema_filename) {
    cout << "Попытка загрузить файл схемы: " << schema_filename << endl;
    ifstream schema_file(schema_filename);
    if (!schema_file.is_open()) {
        cerr << "Ошибка: не удалось открыть файл схемы " << schema_filename << endl;
        return false;
    }

    try {
        schema_file >> schema_data;
    } catch (const json::parse_error& e) {
        cerr << "Ошибка парсинга JSON: " << e.what() << endl;
        return false;
    }

    cout << "Схема успешно загружена." << endl;
    return true;
}

bool executeInsert(const string& sql_query) {
    cout << "Получен запрос: " << sql_query << endl;

    string word;
    stringstream ss(sql_query);

    ss >> word;
    if (word != "INSERT") {
        cerr << "Ошибка: неверный синтаксис INSERT." << endl;
        return false;
    }
    ss >> word;
    if (word != "INTO") {
        cerr << "Ошибка: неверный синтаксис INSERT." << endl;
        return false;
    }

    string table_name;
    ss >> table_name;
    cout << "Имя таблицы: " << table_name << endl;

    ss >> word;
    if (word != "VALUES") {
        cerr << "Ошибка: неверный синтаксис INSERT, отсутствует VALUES." << endl;
        return false;
    }

    while (ss.peek() == ' ') {
        ss.ignore();
    }

    if (ss.peek() != '(') {
        cerr << "Ошибка: неверный синтаксис INSERT, отсутствует '('." << endl;
        return false;
    }

    ss.ignore();  // Игнорируем '('

    DualLinkedList valuesList;
    valuesList.init();
    string value;
    char delimiter = ',';

    cout << "Начало обработки значений..." << endl;

    while (true) {
        // Пропускаем пробелы перед '('
        while (ss.peek() == ' ') {
            ss.ignore();
        }

        if (ss.peek() == ')') {
            ss.ignore();
            break;  // Выходим, если нашли закрывающую скобку
        }

        getline(ss, value, delimiter);

        if (value.empty()) {
            cerr << "Ошибка: пустое значение." << endl;
            return false;
        }

        // Удаляем пробелы и кавычки
        size_t first = value.find_first_not_of(' ');
        size_t last = value.find_last_not_of(' ');

        if (first == string::npos || last == string::npos) {
            cerr << "Ошибка: значение состоит только из пробелов." << endl;
            return false;
        }

        value = value.substr(first, (last - first));
        cout << "value "<< value<<endl;
        

        // Убираем одинарные кавычки, если они есть
        string clean_value;
        for (char c : value) {
            if (c != '\'' and c != ')') {
                clean_value += c;
            }
        }

        // Добавляем очищенное значение в список
        valuesList.addToTail(clean_value);
        cout << "Добавлено значение: " << clean_value << endl;

        // Проверяем, достигли ли конца строки
        if (ss.eof()) {
            break;  // Если конец потока, выходим из цикла
        }
    }

    cout << "Все значения успешно обработаны." << endl;

    string schema_name = schema_data.value("name", "");
    string base_csv_filename = schema_name + "/" + table_name + "/";

    int tuples_limit = schema_data.value("tuples_limit", 10);

    // Определяем текущий доступный CSV-файл
    int file_number = 1;
    int total_row_count = 0;  // Счётчик всех строк во всех файлах
    string csv_filename = base_csv_filename + to_string(file_number) + ".csv";

    // Ищем последний файл с данными и проверяем количество строк
    while (fs::exists(csv_filename)) {
        ifstream csv_file(csv_filename);
        if (!csv_file.is_open()) {
            cerr << "Ошибка: не удалось открыть файл CSV " << csv_filename << " для чтения." << endl;
            return false;
        }

        string line;
        int row_count = 0;  // Счётчик строк в текущем файле

        // Пропускаем заголовок и считаем строки
        if (getline(csv_file, line)) {
            while (getline(csv_file, line)) {
                row_count++;
            }
        }
        csv_file.close();

        total_row_count += row_count; 

        // Если текущий файл переполнен, переходим к следующему файлу
        if (row_count >= tuples_limit) {
            file_number++;
            csv_filename = base_csv_filename + to_string(file_number) + ".csv";
        } else {
            break;
        }
    }

    cout << "Работаем с файлом: " << csv_filename << endl;
    cout << "Текущее количество строк во всех файлах: " << total_row_count << endl;

    // Открываем файл для добавления новой строки (append)
    ofstream output_csv;
    if (fs::exists(csv_filename)) {
        output_csv.open(csv_filename, ios::app);
    } else {
        // Если файл не существует, создаём его и записываем заголовок
        output_csv.open(csv_filename);
        if (!output_csv.is_open()) {
            cerr << "Ошибка: не удалось создать файл " << csv_filename << "!" << endl;
            return false;
        }

        // Записываем заголовки столбцов без кавычек
        const auto& columns = schema_data["structure"][table_name];
            for (size_t i = 0; i < columns.size(); ++i) {
                output_csv << columns[i].get<string>();
                if (i < columns.size() - 1) {
                    output_csv << ",";
                }
            }
        output_csv << endl;
    }

    if (!output_csv.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << csv_filename << " для записи!" << endl;
        return false;
    }

    // Записываем остальные значения из списка
    DualListNode* current_value = valuesList.head;
    while (current_value != nullptr) {
        cout << "Запись значения: " << current_value->data << endl;

        output_csv << current_value->data;

        if (current_value->next != nullptr) {
            output_csv << ",";
        }

        current_value = current_value->next;
    }

    output_csv << endl;
    output_csv.close();

    cout << "Все значения успешно записаны в CSV." << endl;
    return true;
}

bool executeDelete(const string& sql_query) {
    cout << "Получен запрос: " << sql_query << endl;

    string word;
    stringstream ss(sql_query);

    // Обрабатываем SQL запрос
    ss >> word; // Пропускаем "DELETE"
    if (word != "DELETE") {
        cerr << "Ошибка: неверный синтаксис DELETE." << endl;
        return false;
    }

    ss >> word; // Пропускаем "FROM"
    if (word != "FROM") {
        cerr << "Ошибка: неверный синтаксис DELETE." << endl;
        return false;
    }

    // Получаем имя таблицы
    string table_name;
    ss >> table_name;
    cout << "Имя таблицы: " << table_name << endl;

    ss >> word; // Пропускаем "WHERE"
    if (word != "WHERE") {
        cerr << "Ошибка: неверный синтаксис DELETE, отсутствует WHERE." << endl;
        return false;
    }

    // Получаем условие WHERE
    string column_name, value;
    getline(ss, column_name, '=');

    // Убираем пробелы из column_name
    column_name.erase(column_name.find_last_not_of(" \t\n\r\f\v") + 1);
    column_name.erase(0, column_name.find_first_not_of(" \t\n\r\f\v"));
    cout << "Имя колонки: " << column_name << endl;

    // Игнорируем пробелы после знака "="
    ss >> ws;

    // Получаем значение
    getline(ss, value);
    if (value.front() == '\'' && value.back() == '\'') {
        value = value.substr(1, value.length() - 2); // Убираем кавычки
    }
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
    cout << "Условие WHERE: " << column_name << " = '" << value << "'" << endl;

    string schema_name = schema_data.value("name", "");
    string base_csv_filename = schema_name + "/" + table_name + "/";

    int tuples_limit = schema_data.value("tuples_limit", 10);

    // Ищем файлы CSV в таблице
    int file_number = 1;
    string csv_filename = base_csv_filename + to_string(file_number) + ".csv";
    int column_index = -1;
    bool column_found = false;

    // Временный двусвязный список для хранения всех строк, которые нужно сохранить
    DualLinkedList remaining_rows;
    remaining_rows.init();
    string header_line;

    // Перебор всех CSV-файлов
    while (fs::exists(csv_filename)) {
        ifstream csv_file(csv_filename);
        if (!csv_file.is_open()) {
            cerr << "Ошибка: не удалось открыть файл " << csv_filename << " для чтения." << endl;
            return false;
        }

        if (header_line.empty()) {
            getline(csv_file, header_line);  // Читаем заголовок один раз (из первого файла)
        } else {
            // Пропускаем заголовок в остальных файлах
            string skip_header;
            getline(csv_file, skip_header);
        }

        stringstream header_stream(header_line);
        string header_cell;
        int current_column_index = 0;

        // Определяем индекс колонки для фильтрации (только один раз)
        if (!column_found) {
            while (getline(header_stream, header_cell, ',')) {
                if (header_cell == column_name) {
                    column_index = current_column_index;
                    column_found = true;
                    break;
                }
                current_column_index++;
            }

            if (column_index == -1) {
                cerr << "Ошибка: столбец " << column_name << " не найден в таблице " << table_name << endl;
                return false;
            }
        }

        string line;
        while (getline(csv_file, line)) {
            stringstream row_stream(line);
            string cell;
            int current_index = 0;
            string target_cell;

            // Ищем значение в нужной колонке
            while (getline(row_stream, cell, ',')) {
                if (current_index == column_index) {
                    target_cell = cell;
                    break;
                }
                current_index++;
            }

            // Убираем кавычки из значения
            if (target_cell.front() == '\'' && target_cell.back() == '\'') {
                target_cell = target_cell.substr(1, target_cell.length() - 2);
            }
            target_cell.erase(0, target_cell.find_first_not_of(' '));
            target_cell.erase(target_cell.find_last_not_of(' ') + 1);

            // Пропускаем строку, если нужно её удалить
            if (target_cell == value) {
                continue;  // Удаляем строку
            }

            // Если строка не удалена, сохраняем её для последующей записи
            remaining_rows.addToTail(line);
        }

        csv_file.close();

        // Удаляем исходный файл
        fs::remove(csv_filename);

        file_number++;
        csv_filename = base_csv_filename + to_string(file_number) + ".csv";
    }

    // Теперь записываем оставшиеся строки обратно в файлы
    file_number = 1;
    csv_filename = base_csv_filename + to_string(file_number) + ".csv";
    int current_row_count = 0;
    ofstream output_csv(csv_filename);

    if (!output_csv.is_open()) {
        cerr << "Ошибка: не удалось создать файл " << csv_filename << "!" << endl;
        return false;
    }

    // Записываем заголовок
    output_csv << header_line << endl;

    // Записываем оставшиеся строки из списка
    DualListNode* current_node = remaining_rows.head;
    while (current_node != nullptr) {
        if (current_row_count >= tuples_limit) {
            output_csv.close();
            file_number++;
            csv_filename = base_csv_filename + to_string(file_number) + ".csv";
            current_row_count = 0;

            output_csv.open(csv_filename);
            if (!output_csv.is_open()) {
                cerr << "Ошибка: не удалось создать файл " << csv_filename << "!" << endl;
                return false;
            }

            output_csv << header_line << endl;  // Записываем заголовок в новый файл
        }

        // Записываем строку без изменений
        output_csv << current_node->data << endl;
        current_row_count++;

        current_node = current_node->next;
    }

    output_csv.close();

    // Уничтожаем двусвязный список
    remaining_rows.destroy();

    cout << "Удаление завершено и данные успешно перераспределены." << endl;
    return true;
}


string getColumnValues(const string& row, DualLinkedList& column_indexes) {
    stringstream row_stream(row);
    string value;
    string result;

    DualListNode* current_index_node = column_indexes.head;
    int current_index = 0;

    // Пробегаем по строке, разделенной запятыми
    while (getline(row_stream, value, ',')) {
        
        // Проверяем, соответствует ли текущий индекс индексу колонки, которую нужно вернуть
        if (current_index_node && stoi(current_index_node->data) == current_index) {
            
            if (!result.empty()) {
                result += ",";  // Добавляем запятую, если это не первое значение
            }
            result += value;  // Добавляем значение колонки

            current_index_node = current_index_node->next;

            if (!current_index_node) {
                break;
            }
        }
        current_index++;
    }

    return result;
}

void findColumnIndexes(const DualLinkedList& columnsList, const string& header_line, DualLinkedList& column_indexes) {
    DualListNode* current_column = columnsList.head;

    // Цикл по каждой колонке в списке
    while (current_column != nullptr) {
        stringstream header_stream(header_line);
        string header_column;
        int current_index = 0;
        bool column_found = false;

        // Цикл по каждой колонке в заголовке CSV
        while (getline(header_stream, header_column, ',')) {
            header_column.erase(0, header_column.find_first_not_of(" \t\n\r\f\v"));
            header_column.erase(header_column.find_last_not_of(" \t\n\r\f\v") + 1);

            // Если колонка из списка найдена в заголовке CSV
            if (header_column == current_column->data) {
                column_indexes.addToTail(to_string(current_index));
                column_found = true;
                break; 
            }
            current_index++;
        }

        if (!column_found) {
            cerr << "Ошибка: колонка " << current_column->data << " не найдена в заголовке CSV." << endl;
        }

        current_column = current_column->next;
    }
}

bool executeSelect(const string& sql_query) {
    cout << "Получен запрос: " << sql_query << endl;

    string word;
    stringstream ss(sql_query);

    // 1. Проверка на ключевое слово "SELECT"
    ss >> word;
    if (word != "SELECT") {
        cerr << "Ошибка: неверный синтаксис SELECT." << endl;
        return false;
    }

    // 2. Чтение списка колонок
    string column_list;
    getline(ss, column_list, 'F');  // Читаем всё до 'F' в "FROM"
    
    ss.unget();  // Вернём 'F' обратно в поток для корректной обработки "FROM"

    // Удаляем пробелы в начале и конце списка колонок
    column_list.erase(0, column_list.find_first_not_of(" \t\n\r\f\v"));
    column_list.erase(column_list.find_last_not_of(" \t\n\r\f\v") + 1);

    // 3. Проверка на ключевое слово "FROM"
    ss >> word;
    if (word != "FROM") {
        cerr << "Ошибка: отсутствует ключевое слово FROM." << endl;
        return false;
    }

    // 4. Чтение списка таблиц
    string table_name;
    ss >> table_name;

    cout << "Таблица: " << table_name << endl;

    // 5. Определение, есть ли точка в запросе
    bool has_filter = (sql_query.find('.') != string::npos);
    
    // --- Обработка запроса с фильтрацией ---
if (has_filter) {
        cout << "Обнаружена фильтрация по колонкам с точкой." << endl;
        string schema_name = schema_data.value("name", "");
    // 6. Парсинг условия WHERE
    string condition;
    ss >> word;  // Пропускаем "WHERE"
    getline(ss, condition);
    
    ifstream csv_file1; 
    ifstream csv_file2;

     // 7. Разделение условия на части
    string left_part, logical_operator, right_part;
    stringstream condition_ss(condition);

    string temp_word;
while (condition_ss >> temp_word) {
    if (temp_word == "AND" || temp_word == "OR") { // Если встретили логический оператор, останавливаемся
        logical_operator = temp_word; // Запоминаем оператор
        break;
    }
    left_part += temp_word + " ";
}
left_part.pop_back(); // Удаляем лишний пробел в конце

    getline(condition_ss, right_part); //  Правая  часть

    // 8.  Парсинг  левой  и  правой  частей  условия
        string left_table_column, right_table_column, left_value, right_value;

        // --- Парсинг левой части ---
stringstream left_ss(left_part);
getline(left_ss, left_table_column, '=');  // Читаем до "="

// Проверяем, не осталось ли слово "WHERE" в начале
if (left_table_column.find("WHERE") != string::npos) {
    left_table_column.erase(0, left_table_column.find("WHERE") + 5); // Удаляем "WHERE"
}

// Убираем пробелы и кавычки из левой части
left_table_column.erase(0, left_table_column.find_first_not_of(" \t\n\r\f\v"));
left_table_column.erase(left_table_column.find_last_not_of(" \t\n\r\f\v") + 1);

// Читаем значение
left_ss >> ws >> left_value >> ws;

// Убираем пробелы и кавычки из левого значения
left_value.erase(0, left_value.find_first_not_of(" \t\n\r\f\v"));
left_value.erase(left_value.find_last_not_of(" \t\n\r\f\v") + 1);
if (!left_value.empty() && left_value.front() == '\'' && left_value.back() == '\'') {
    left_value = left_value.substr(1, left_value.length() - 2);  // Удаляем кавычки
}
 cout << "Левая таблица.колонка: " << left_table_column << endl; // ОТЛАДКА
        cout << "Левое значение: " << left_value << endl; // ОТЛАДКА
// --- Парсинг правой части ---
stringstream right_ss(right_part);
getline(right_ss, right_table_column, '=');  // Читаем до "="
right_ss >> ws >> right_value >> ws;  // Читаем значение

// Убираем пробелы и кавычки из правой части
right_table_column.erase(0, right_table_column.find_first_not_of(" \t\n\r\f\v"));
right_table_column.erase(right_table_column.find_last_not_of(" \t\n\r\f\v") + 1);
right_value.erase(0, right_value.find_first_not_of(" \t\n\r\f\v"));
right_value.erase(right_value.find_last_not_of(" \t\n\r\f\v") + 1);

if (!right_value.empty() && right_value.front() == '\'' && right_value.back() == '\'') {
    right_value = right_value.substr(1, right_value.length() - 2);  // Удаляем кавычки
}
  cout << "Правая таблица.колонка: " << right_table_column << endl; // ОТЛАДКА
        cout << "Правое значение: " << right_value << endl; // ОТЛАДКА

    cout << "Левая часть: " << left_table_column << " = " << left_value << endl;
    cout << "Логический оператор: " << logical_operator << endl;
    cout << "Правая часть: " << right_table_column << " = " << right_value << endl;


    string left_table_name = left_table_column.substr(0, left_table_column.find('.')); 
    string right_table_name = right_table_column.substr(0, right_table_column.find('.')); 
    
    DualLinkedList left_table_data;  //  Данные  левой  таблицы
    left_table_data.init();
    DualLinkedList right_table_data;  //  Данные  правой  таблицы
    right_table_data.init();
    DualLinkedList left_column_indexes;  //  Индексы  колонок  левой  таблицы
    left_column_indexes.init();
    DualLinkedList right_column_indexes;  //  Индексы  колонок  правой  таблицы
    right_column_indexes.init();

    // ---  Чтение  данных  из  левой  таблицы  ---
    int current_file_index1 = 1;
    string csv_filename1 = schema_name + "/" + left_table_name + "/" + to_string(current_file_index1) + ".csv"; 
    string header_line1;
    
    // ---  Цикл  чтения  CSV-файлов  левой  таблицы  ---
while (fs::exists(csv_filename1)) {
    cout << "Чтение данных из файла: " << csv_filename1 << endl;  //  ОТЛАДКА
    ifstream csv_file1(csv_filename1);
    if (!csv_file1.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << csv_filename1 << " для чтения." << endl;
        return false;
    }

    getline(csv_file1, header_line1);
    if (header_line1.empty()) {
            // Вызов  findColumnIndexes  для  левой  таблицы
            findColumnIndexes(left_table_data, header_line1, left_column_indexes);  
        }

    cout << "Заголовок: " << header_line1 << endl;  //  ОТЛАДКА
    

    string line1;
    while (getline(csv_file1, line1)) {
        left_table_data.addToTail(line1);  //  Добавляем  строку  в  список
        cout << "Прочитана строка: " << line1 << endl;  //  ОТЛАДКА
    }

    csv_file1.close();
    current_file_index1++;
    csv_filename1 = schema_name + "/" + left_table_name + "/" + to_string(current_file_index1) + ".csv";
}


string left_column_name = left_table_column.substr(left_table_column.find('.') + 1);  //  Извлекаем  имя  колонки
string right_column_name = left_value.substr(left_value.find('.') + 1);  //  Извлекаем  имя  колонки

DualLinkedList temp_list;
temp_list.init();
temp_list.addToTail(left_column_name); //  Добавляем  left_column_name  в  список
findColumnIndexes(temp_list, header_line1, left_column_indexes);

DualLinkedList temp_list2;
temp_list2.init();
temp_list2.addToTail(right_column_name);  
// --- Чтение данных из правой таблицы ---
int current_file_index2 = 1;
string csv_filename2 = schema_name + "/" + right_table_name + "/" + to_string(current_file_index2) + ".csv";
string header_line2;

while (fs::exists(csv_filename2)) {
    cout << "Чтение данных из файла (правая таблица): " << csv_filename2 << endl; // ОТЛАДКА
    csv_file2.open(csv_filename2); 
    if (!csv_file2.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << csv_filename2 << " для чтения." << endl;
        return false;
    }

    // Чтение заголовка, если он еще не прочитан
    if (header_line2.empty()) { 
        getline(csv_file2, header_line2);
        cout << "Заголовок (правая таблица): " << header_line2 << endl; // ОТЛАДКА

        // Вызов findColumnIndexes для правой таблицы
        DualLinkedList temp_list2;
        temp_list2.init();
        temp_list2.addToTail(right_column_name); 
        findColumnIndexes(temp_list2, header_line2, right_column_indexes); 
    }

    string line2;
    while (getline(csv_file2, line2)) {
        right_table_data.addToTail(line2); 
        cout << "Прочитана строка (правая таблица): " << line2 << endl;
    }

    csv_file2.close();
    current_file_index2++;
    csv_filename2 = schema_name + "/" + right_table_name + "/" + to_string(current_file_index2) + ".csv";
}

cout << "Поиск индекса колонки '" << left_table_column << "' в заголовке левой таблицы..." << endl;  // ОТЛАДКА

cout << "Индекс колонки левой таблицы: ";
left_column_indexes.print();  // ОТЛАДКА
cout << "Индекс колонки правой таблицы: ";
right_column_indexes.print();  // ОТЛАДКА


// 10.  Фильтрация  строк
DualListNode* current_row_left = left_table_data.head;
set<string> rows;
while (current_row_left != nullptr) {
    DualListNode* current_row_right = right_table_data.head;
    while (current_row_right != nullptr) {

        string left_column_value = getColumnValues(current_row_left->data, left_column_indexes);
        string right_column_value = getColumnValues(current_row_right->data, right_column_indexes);

    string left_value_table = left_value.substr(0, left_value.find('.')); 
    string left_value_column = left_value.substr(left_value.find('.') + 1); 
        DualLinkedList left_value_column_indexes;
        left_value_column_indexes.init();
        string header_line_for_left_value = (left_value_table == left_table_name) ? header_line1 : header_line2;

        DualLinkedList left_temp_list;
        left_temp_list.init();
        left_temp_list.addToTail(left_column_name); 


        findColumnIndexes(left_temp_list, header_line2, left_value_column_indexes);
        string actual_left_value = getColumnValues(current_row_right->data, left_value_column_indexes); 

        bool left_condition_result = (left_column_value == right_column_value); 
        bool right_condition_result = (actual_left_value == right_value); 
        bool condition_result = false; 

        if (logical_operator == "AND") {
            condition_result = left_condition_result && right_condition_result; 
        } else if (logical_operator == "OR") {
            condition_result = left_condition_result || right_condition_result; 
        } else {
            cerr << "Ошибка: неизвестный логический оператор: " << logical_operator << endl;
            return false;
        }

        if (condition_result) {            
            rows.insert(current_row_left->data);
            rows.insert(current_row_right->data);
        }

        current_row_right = current_row_right->next;
    }
    current_row_left = current_row_left->next;

            

}
    for (string n : rows)
        std::cout << n << endl;
    }
    else {
    cout << "Запрос без фильтрации, выполняется обычная выборка." << endl;
     cout << "Получен запрос: " << sql_query << endl;

    string word;
    stringstream ss(sql_query);

    // 1. Проверка на ключевое слово "SELECT"
    ss >> word;
    if (word != "SELECT") {
        cerr << "Ошибка: неверный синтаксис SELECT." << endl;
        return false;
    }

    // 2. Чтение списка колонок
    string column_list;
    getline(ss, column_list, 'F');  // Читаем всё до 'F' в "FROM"
    
    ss.unget();  // Вернём 'F' обратно в поток для корректной обработки "FROM"

    // Удаляем пробелы в начале и конце списка колонок
    column_list.erase(0, column_list.find_first_not_of(" \t\n\r\f\v"));
    column_list.erase(column_list.find_last_not_of(" \t\n\r\f\v") + 1);

    // 3. Проверка на ключевое слово "FROM"
    ss >> word;
    if (word != "FROM") {
        cerr << "Ошибка: отсутствует ключевое слово FROM." << endl;
        return false;
    }

    // 4. Чтение первой таблицы
    string table_name1, table_name2;
    ss >> table_name1;

    // Убираем запятую в конце имени таблицы, если она есть
    if (!table_name1.empty() && table_name1.back() == ',') {
        table_name1.pop_back();
    }

    cout << "Таблица 1: " << table_name1 << endl;

    // Проверяем, есть ли в запросе вторая таблица
    bool two_tables = false;
    ss >> word;
    
    // Отладочное сообщение, чтобы увидеть, что мы считали после имени первой таблицы
    cout << "После имени первой таблицы считали: '" << word << "'" << endl;

    // Проверяем, является ли это слово именем таблицы в структуре схемы
    if (schema_data["structure"].contains(word)) {
        table_name2 = word;
        two_tables = true;
        cout << "Считанное имя второй таблицы: '" << table_name2 << "'" << endl;
    } else {
        cout << "Вторая таблица не указана, или её имя не найдено в схеме. Работать будем только с одной таблицей." << endl;
    }

    // 5. Разбор списка колонок
    DualLinkedList columnsList1;
    columnsList1.init();
    stringstream columns_stream1(column_list);
    string column;

    cout << "Начало обработки колонок..." << endl;
    while (getline(columns_stream1, column, ',')) {
        // Удаляем пробелы в начале и конце имени колонки
        column.erase(0, column.find_first_not_of(" \t\n\r\f\v"));
        column.erase(column.find_last_not_of(" \t\n\r\f\v") + 1);
        
        if (column.empty()) {
            cerr << "Ошибка: пустое имя колонки." << endl;
            return false;
        }

        // Добавляем колонку в список
        columnsList1.addToTail(column);
        cout << "Добавлена колонка: " << column << endl;
    }

    // 6. Проверка, что колонки были успешно прочитаны
    if (columnsList1.head == nullptr) {
        cerr << "Ошибка: список колонок пуст." << endl;
        return false;
    }
    cout << "Добавлены следующие колонки: ";
    string schema_name = schema_data["name"];

    if (column == "*") {
        // Проверяем, указана ли таблица в схеме
        if (!schema_data["structure"]["tables"].contains(table_name1)) {
            cerr << "Ошибка: таблица " << table_name1 << " не найдена в схеме." << endl;
            return false;
        }

        string order_table_path = schema_name + "/" + table_name1 + "/" + "1.csv";
        cout <<" order_table_path "<<order_table_path<<endl;
        while (fs::exists(order_table_path)) {
            ifstream order_file(order_table_path);
            if (!order_file.is_open()) {
                cerr << "Ошибка: не удалось открыть файл с ордерами." << endl;
                return false;
            }

            string line;
            while (getline(order_file, line)) {
                cout << line << endl;
            }
            order_file.close();
            break;
        }

        return true;
    } 



DualListNode* check_node = columnsList1.head;
while (check_node != nullptr) {
    cout << check_node->data << " ";
    check_node = check_node->next;
}
cout << endl;

    // 7. Получение имени схемы и пределов кортежей
    if (schema_name.empty()) {
        cerr << "Ошибка: имя схемы не найдено в schema.json." << endl;
        return false;
    }
    int tuples_limit = schema_data["tuples_limit"];

    // 8. Открытие первой таблицы и обработка данных
    int current_file_index1 = 1;
    string csv_filename1 = schema_name + "/" + table_name1 + "/" + to_string(current_file_index1) + ".csv";

    ifstream csv_file1;
    string header_line1;

    DualLinkedList table1_data;
    table1_data.init();
    DualLinkedList column_indexes1;
    column_indexes1.init();
    bool first_file1_read = false; 
    while (fs::exists(csv_filename1)) {
        cout << "Чтение данных из файла первой таблицы: " << csv_filename1 << endl;

        csv_file1.open(csv_filename1);
        if (!csv_file1.is_open()) {
            cerr << "Ошибка: не удалось открыть файл " << csv_filename1 << endl;
            return false;
        }

        column_indexes1.init();

        if (header_line1.empty()) {
            getline(csv_file1, header_line1);  // Чтение заголовка один раз
        }

        // Находим индексы колонок в заголовке первой таблицы
        findColumnIndexes(columnsList1, header_line1, column_indexes1);
        
        string line1;
        if (first_file1_read) { 
            getline(csv_file1, line1); 
        }
        while (getline(csv_file1, line1)) {
            stringstream line_stream1(line1);
            string value;
            int current_index = 0;
            DualListNode* current_index_node = column_indexes1.head;

            cout << "Значения из первой таблицы: ";
            while (current_index_node != nullptr) {
                stringstream line_stream1_copy(line1);  
                for (int i = 0; i <= stoi(current_index_node->data); ++i) {
                    getline(line_stream1_copy, value, ',');
                }
                cout << value << " ";
                current_index_node = current_index_node->next;
            }
            cout << endl;
            table1_data.addToTail(line1);
        }

        csv_file1.close();
        current_file_index1++;
        first_file1_read = true;
        csv_filename1 = schema_name + "/" + table_name1 + "/" + to_string(current_file_index1) + ".csv";
    }

    DualLinkedList table2_data;
    table2_data.init();
    DualLinkedList column_indexes2;
    column_indexes2.init();
    bool first_file2_read = false;
    // 9. Открытие второй таблицы, если указана

    if (two_tables) {
        int current_file_index2 = 1;
        string csv_filename2 = schema_name + "/" + table_name2 + "/" + to_string(current_file_index2) + ".csv";

        ifstream csv_file2;
        string header_line2;

        while (fs::exists(csv_filename2)) {
            cout << "Чтение данных из файла второй таблицы: " << csv_filename2 << endl;


            if (!fs::exists(csv_filename2)) {  
                cout << "Файл " << csv_filename2 << " не найден, выход из цикла." << endl;
                break; 
            }

            csv_file2.open(csv_filename2);
            if (!csv_file2.is_open()) {
                cerr << "Ошибка: не удалось открыть файл " << csv_filename2 << endl;
                return false;
            }

            if (header_line2.empty()) {
                getline(csv_file2, header_line2);
                cout << "Заголовок второй таблицы: " << header_line2 << endl;
            }

            DualLinkedList columnsList2;
            columnsList2.init();
            stringstream columns_stream2(column_list);
            string column;
            if (first_file2_read) { 
                getline(columns_stream2, column); 
        }
            cout << "Начало обработки колонок для второй таблицы..." << endl;
            while (getline(columns_stream2, column, ',')) {
                // Удаляем пробелы в начале и конце имени колонки
                column.erase(0, column.find_first_not_of(" \t\n\r\f\v"));
                column.erase(column.find_last_not_of(" \t\n\r\f\v") + 1);

                if (column.empty()) {
                    cerr << "Ошибка: пустое имя колонки." << endl;
                    return false;
                }

                columnsList2.addToTail(column);
                cout << "Добавлена колонка: " << column << endl;
            }

            // Находим индексы колонок в заголовке второй таблицы
            findColumnIndexes(columnsList2, header_line2, column_indexes2);

            string line2;
            while (getline(csv_file2, line2)) {
                table2_data.addToTail(line2); 
            }

            csv_file2.close();
            current_file_index2++;
            first_file2_read = true;
            csv_filename2 = schema_name + "/" + table_name2 + "/" + to_string(current_file_index2) + ".csv";
            cout << "Переход к следующему файлу: " << csv_filename2 << endl; 
        }
    }

    // 10. Декартово произведение строк из двух таблиц с выбором колонок
    if (two_tables) {
        DualListNode* row1 = table1_data.head;
        while (row1 != nullptr) {
            DualListNode* row2 = table2_data.head;
            while (row2 != nullptr) {
                cout << "Декартово произведение строк: ";
                string row1_columns = getColumnValues(row1->data, column_indexes1);  // Получаем только нужные колонки
                string row2_columns = getColumnValues(row2->data, column_indexes2);  // Получаем только нужные колонки
                cout << row1_columns << " | " << row2_columns << endl;
                row2 = row2->next;
            }
            row1 = row1->next;
        }
    } else {
// Если только одна таблица, просто выводим её данные с выбором колонок
    DualListNode* row1 = table1_data.head;
    while (row1 != nullptr) {
        string row1_columns = getColumnValues(row1->data, column_indexes1);  // Получаем все нужные колонки
        cout << row1_columns << endl;
        row1 = row1->next;
    }

    cout << "Запрос SELECT выполнен успешно." << endl;
    return true;
    }


}
return true;
}