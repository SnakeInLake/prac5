#pragma once
#include <string>


void start_server();

bool lock_table(const std::string& table_name);
bool unlock_table(const std::string& table_name);

std::string getTableName(const std::string& sql_query);

std::string process_query(const std::string& sql_query); 
