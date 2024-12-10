#pragma once
#include <iostream>
#include <sstream>
#include <string>

class OutputCapturer {
public:
    OutputCapturer() {
        // Сохраняем старый буфер cout
        old_buf = std::cout.rdbuf();
        // Устанавливаем новый буфер
        std::cout.rdbuf(buffer.rdbuf());
    }

    ~OutputCapturer() {
        // Восстанавливаем старый буфер cout
        std::cout.rdbuf(old_buf);
    }

    std::string getCapturedOutput() const {
        return buffer.str();
    }

private:
    std::stringstream buffer;
    std::streambuf* old_buf;
};