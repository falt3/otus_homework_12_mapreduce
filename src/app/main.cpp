#include <iostream>
#include <string>
#include <filesystem>
#include <functional>

#include "mapreduce.h"

namespace fs = std::filesystem;


void func_map(size_t prefix, IReader& reader, MapReduce::TypeList& list_map) 
{
    while (true) {
        std::string str = reader.getData(); 
        if (str.empty()) break;
        
        if (str.size() < prefix) continue;
        str.erase(prefix);
        std::transform(str.begin(), str.end(), str.begin(), ::tolower);
        list_map.push_back(std::move(str));
    }
}


void func_reduce(MapReduce::TypeList& list_reduceIn, MapReduce::TypeList& list_reduceOut) 
{
    if (list_reduceIn.size() == 0) return;
    else if (list_reduceIn.size() == 1) {
        list_reduceOut.push_back(list_reduceIn.back() + " 1");
        return;
    }

    // Сортировка (группировка)
    std::sort(list_reduceIn.begin(), list_reduceIn.end());

    // Подсчет совпадений и формирование выходного списка вида: "email 2"
    size_t countRepeat = 1;
    auto it1 = list_reduceIn.begin();
    for (auto it2 = it1 + 1; it2 != list_reduceIn.end(); ++it1, ++it2) {
        if (*it1 == *it2) countRepeat++;
        else {
            list_reduceOut.push_back(*it1 + " " + std::to_string(countRepeat));
            countRepeat = 1;
        }
    }
    list_reduceOut.push_back(*it1 + " " + std::to_string(countRepeat));
}



int main(int argc, const char* argv[])
{
    std::string fnIn = "../tests/in2.txt";
    std::string fnOut = "outreduce.txt";
    size_t countMaps = 1;
    size_t countReduces = 2;
    
    if (argc == 4) {
        fnIn = argv[1];
        try {
            countMaps = std::stoi(argv[2]);
            countReduces = std::stoi(argv[3]);
        }
        catch(...) {
            std::cout << "Неверное значение в параметрах\n";
            return 1;
        }
    }
    else if (argc != 1) {
        std::cout << "Неверное количество параметров\n";
        return 1;
    }

    fs::path path_fileIn(fnIn);
    if (!fs::is_regular_file(path_fileIn)) {
        std::cout << "Файл не найден\n";
        return 2;
    }

    using namespace std::placeholders;

    size_t prefix = 1;
    while (1) {
        auto f_map = std::bind(func_map, prefix, _1, _2);
        MapReduce::execute(fnIn, fnOut, countMaps, countReduces, f_map, func_reduce);
    

        // проверка на получение правильного решения
        bool res = true;
        size_t countItems = 0;
        std::ifstream ifile(fnOut);
        if (ifile.is_open()) {
            while (!ifile.eof()) {
                std::string str;
                size_t n = 0;
                ifile >> str >> n;
                if (n != 0) countItems++;
                if (n > 1) {
                    res = false;
                    break;
                }
            }
            ifile.close();
        }

        if (countItems == 0) {
            std::cout << "В файле недостоверные данные" << std::endl;
            break;
        }
        else if (res) {
            std::cout << "Префикс = " << prefix << std::endl;                
            break;
        }
        else 
            prefix++;
    }

    return 0;
}