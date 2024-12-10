#pragma once
#include <string>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;

// Функция для обработки JSON-запроса и генерации SQL-команды
string generateInsertSQLUser(const string& command);

