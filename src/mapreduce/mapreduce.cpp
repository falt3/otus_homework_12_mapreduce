#include "mapreduce.h"
#include <iostream>
#include <thread>
#include <memory>
#include <fstream>
#include <algorithm>
#include <functional>



void MapReduce::execute(const std::string& filenameIn, const std::string& filenameOut, 
                        size_t countMaps, size_t countReduces, 
                        Func_map func_map, Func_reduce func_reduce)
{
    //------- input splits ------------
    std::vector<size_t> list_pos;       // список начала позиций откуда надо читать информацию
    list_pos.resize(countMaps + 1);     // +1 - добавляем размер файла
    {
        std::ifstream ifs(filenameIn, std::ios::ate);
        size_t sizeFile = ifs.tellg();      // размер файла

        size_t sizePart = sizeFile / countMaps;  // размер части файла
        ifs.seekg(0);
        for (int i = 0; i < list_pos.size() - 1; ++i) {
            if (i != 0) {                   // выравниваем по переносу строки
                ifs.seekg(sizePart*i - 1);
                while (ifs.peek() != '\n')  
                    ifs.seekg(static_cast<size_t>(ifs.tellg()) + 1);
                ifs.seekg(static_cast<size_t>(ifs.tellg()) + 1);
            } 
            list_pos[i] = ifs.tellg();
        }
        list_pos[countMaps] = sizeFile;    
    }


    //------- mapping ------------
    // std::cout << "---- mapping ---\n";
    std::vector<TypeList> lists_map;  
    lists_map.resize(countMaps);
    {
        std::list<ReaderFile> list_readers;
        std::list<std::thread> threads;
        for (size_t i = 0; i < countMaps; ++i) {
            list_readers.emplace_back(filenameIn, list_pos[i], list_pos[i+1]);
            threads.emplace_back(func_map, std::ref(list_readers.back()),/*i, std::move(ss), list_pos[i+1],*/ std::ref(lists_map[i]));
        }
        for (auto& el: threads) {
            el.join();
        }
    }


    //------- shuffling ------------
    // std::cout << "---- shuffling ---\n";
    std::vector<TypeList> lists_reduceIn;      
    lists_reduceIn.resize(countReduces);
    {    
        size_t sizePartReduce = (static_cast<size_t>(0) - 1) / countReduces;
        for (auto& it: lists_map) {
            for (auto& str: it) {
                size_t hash = std::hash<std::string>{}(str);
                size_t numberReduce = hash / sizePartReduce;
                if (numberReduce >= countReduces) 
                    numberReduce = countReduces - 1;
                lists_reduceIn[numberReduce].push_back(str);
            }
        }
    }


    //------- reducer ------------
    // std::cout << "---- reducer ---\n";
    std::vector<TypeList> lists_reduceOut;
    lists_reduceOut.resize(countReduces);    
    {
        std::list<std::thread> threads;
        for (size_t i = 0; i < lists_reduceIn.size(); ++i) {
            threads.emplace_back(func_reduce, std::ref(lists_reduceIn[i]), std::ref(lists_reduceOut[i]));
        }

        for (auto& el: threads) {
            el.join();
        }
    }


    //------- final out ------------
    // std::cout << "---- final out ---\n";
    std::ofstream ofile(filenameOut);
    for(auto& it : lists_reduceOut) {
        for (auto& str: it) 
            ofile << str << std::endl;
    }
    ofile.close();
}
