#pragma once

#include <string>

struct DualListNode {
    std::string data; 
    DualListNode* prev;
    DualListNode* next;
};

struct DualLinkedList {
    DualListNode* head;
    DualListNode* tail;

    void init();
    void addToHead(const std::string& value); 
    void addToTail(const std::string& value); 
    void removeFromHead();
    void removeFromTail();
    void removeByValue(const std::string& value); 
    bool search(const std::string& value); 
    void print();
    void printReverse();
    void saveToFile(const std::string& fileName);
    void loadFromFile(const std::string& fileName);
    void destroy();
    int size() const;
};

