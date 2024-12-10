#pragma once
#include <string>
#include "json.hpp"

using json = nlohmann::json;

extern json schema_data; // Объявление глобальной переменной

bool loadSchema(const std::string& schema_filename); // Объявление функции
bool executeInsert(const std::string& sql_query); // Функция для выполнения INSERT
bool executeDelete(const std::string& sql_query); 
bool executeSelect(const std::string& sql_query);