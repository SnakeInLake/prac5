#pragma once
#include <string>

bool processCommand(const std::string& command);
int findUserIdByKey(const std::string& user_key, const std::string& user_table_path);
bool processDelete(const string& command);
bool processSelect(const string& command);