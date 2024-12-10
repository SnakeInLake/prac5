#include "DualLinkedList.h"
#include <iostream>
#include <fstream>
#include <cstring>

using namespace std;

void DualLinkedList::init() {
    head = nullptr;
    tail = nullptr;
}

void DualLinkedList::addToHead(const string& value) {
    DualListNode* newDualListNode = new DualListNode{value, nullptr, head};
    if (head != nullptr) {
        head->prev = newDualListNode;
    }
    head = newDualListNode;
    if (tail == nullptr) {
        tail = head;
    }
}

void DualLinkedList::addToTail(const string& value) {
    DualListNode* newDualListNode = new DualListNode{value, tail, nullptr};
    if (tail != nullptr) {
        tail->next = newDualListNode;
    }
    tail = newDualListNode;
    if (head == nullptr) {
        head = tail;
    }
}

void DualLinkedList::removeFromHead() {
    if (head == nullptr) {
        cout << "Список пуст." << endl;
        return;
    }

    DualListNode* temp = head;
    head = head->next;
    if (head != nullptr) {
        head->prev = nullptr;
    } else {
        tail = nullptr;
    }
    delete temp;
}

void DualLinkedList::removeFromTail() {
    if (tail == nullptr) {
        cout << "Список пуст." << endl;
        return;
    }

    DualListNode* temp = tail;
    tail = tail->prev;
    if (tail != nullptr) {
        tail->next = nullptr;
    } else {
        head = nullptr;
    }
    delete temp;
}

void DualLinkedList::removeByValue(const string& value) {
    if (head == nullptr) {
        cout << "Список пуст." << endl;
        return;
    }

    DualListNode* temp = head;
    while (temp != nullptr && temp->data != value) {
        temp = temp->next;
    }

    if (temp == nullptr) {
        cout << "Элемент " << value << " не найден." << endl;
        return;
    }

    if (temp->prev != nullptr) {
        temp->prev->next = temp->next;
    } else {
        head = temp->next;
    }

    if (temp->next != nullptr) {
        temp->next->prev = temp->prev;
    } else {
        tail = temp->prev;
    }

    delete temp;
}

bool DualLinkedList::search(const string& value) {
    DualListNode* temp = head;
    while (temp != nullptr) {
        if (temp->data == value) {
            return true;
        }
        temp = temp->next;
    }
    return false;
}

void DualLinkedList::print() {
    DualListNode* temp = head;
    while (temp != nullptr) {
        cout << temp->data << " ";
        temp = temp->next;
    }
    cout << endl;
}

void DualLinkedList::printReverse() {
    DualListNode* temp = tail;
    while (temp != nullptr) {
        cout << temp->data << " ";
        temp = temp->prev;
    }
    cout << endl;
}

void DualLinkedList::destroy() {
    while (head != nullptr) {
        removeFromHead();
    }
}

int DualLinkedList::size() const {
    int count = 0;
    DualListNode* current = head;
    while (current != nullptr) {
        count++;
        current = current->next;
    }
    return count;
}