#include "mapreduce.h"
#include <iostream>
#include <thread>
#include <memory>
#include <fstream>
#include <algorithm>
#include <functional>


void MapReduce::execute()
{
    //------- split ------------
    std::cout << "execute::split\n";
    std::vector<size_t> list_pos;
    list_pos.resize(countMap_ + 1);
    split(filenameIn_, list_pos);

    
    std::cout << "execute::start_map\n";
    std::vector<std::list<std::string>> lists_map;
    lists_map.resize(countMap_);
    map(list_pos, lists_map);


    std::cout << "execute::start_shuffle\n";
    std::vector<std::list<std::string>> lists_reduceIn;
    lists_reduceIn.resize(countReduce_);    
    shuffle(lists_map, lists_reduceIn);
    
    std::cout << "execute::start_reduce\n";
    std::vector<std::list<std::string>> lists_reduceOut;
    lists_reduceOut.resize(countReduce_);    
    reduce(lists_reduceIn, lists_reduceOut);

    std::cout << "execute::exit\n";
}


void work_reduce(size_t i, std::list<std::string>& list_reduceIn, std::list<std::string>& list_reduceOut) 
{
    std::cout << "thread_reduce: " << i << std::endl;
    // std::sort(list_reduce.begin(), list_reduce.end());
    list_reduceIn.sort();

    size_t repeat = 1;
    auto it1 = list_reduceIn.begin();
    auto it2 = it1;
    it2++;
    for (; it2 != list_reduceIn.end(); ++it1, ++it2) {
        if (*it1 == *it2) repeat++;
        else 
            list_reduceOut.push_back(*it1);// + " " + std::to_string(repeat));
    }


    int kk = 0;
    for (auto& it : list_reduceIn) {
        for (auto& str : it)
            std::cout << str << std::endl;
    }    


    int kk = 0;
    for (auto& it : list_reduceOut) {
        for (auto& str : it)
            std::cout << str << std::endl;
    }    
}


void work_map(size_t n, size_t i, std::ifstream&& in, size_t endPos, std::list<std::string>& list_map) 
{
    size_t count = 0;
    while (in.tellg() < endPos - 1) {
        std::string str;
        in >> str;
        
        count++;
        if (str.size() < n) continue;
        str.erase(n);
        std::transform(str.begin(), str.end(), str.begin(), ::tolower);
        list_map.push_back(std::move(str));

        // std::cout << i << ": " << count << ": " << str << std::endl;
    }
    std::cout << "thread_map: " << i << std::endl;
    in.close();
}


void MapReduce::split(const std::string &fn, std::vector<size_t> &list_pos)
{
    std::ifstream ifs(fn, std::ios::ate);
    size_t sizeFile = ifs.tellg();
    std::cout << "split: " << sizeFile << std::endl;

    size_t sizeMap = sizeFile / (list_pos.size() - 1);
    ifs.seekg(0);
    for (int i = 0; i < list_pos.size() - 1; ++i) {
        if (i != 0) {
            ifs.seekg(sizeMap*i - 1);
            while (ifs.peek() != '\n') {
                ifs.seekg(static_cast<size_t>(ifs.tellg()) + 1);
            }
            ifs.seekg(static_cast<size_t>(ifs.tellg()) + 1);
        } 
        list_pos[i] = ifs.tellg();
    }
    list_pos[list_pos.size()-1] = sizeFile;
}


void MapReduce::map(std::vector<size_t>& list_pos, std::vector<std::list<std::string>> &list_map)
{
    std::list<std::thread> threads;
    for (size_t i = 0; i < list_map.size(); ++i) {
        auto ss = std::ifstream(filenameIn_);
        ss.seekg(list_pos[i]);
        
        threads.emplace_back(work_map, 1, i, std::move(ss), list_pos[i+1], std::ref(list_map[i]));
    }

    for (auto& el: threads) {
        el.join();
    }
}


void MapReduce::shuffle(std::vector<std::list<std::string>> &list_map, std::vector<std::list<std::string>>& list_reduce)
{
    size_t countReduce = list_reduce.size();
    size_t partReduce = (static_cast<size_t>(0) - 1) / countReduce;
    for (auto& it: list_map) {
        for (auto& str: it) {
            size_t hash = std::hash<std::string>{}(str);
            size_t numberReduce = hash / partReduce;
            if (numberReduce >= countReduce) 
                numberReduce = countReduce - 1;
            list_reduce[numberReduce].push_back(str);
        }
    }
}


void MapReduce::reduce(std::vector<std::list<std::string>> &list_reduceIn, std::vector<std::list<std::string>> &list_reduceOut)
{
    std::list<std::thread> threads;
    for (size_t i = 0; i < list_reduceIn.size(); ++i) {
        threads.emplace_back(work_reduce, i, std::ref(list_reduceIn[i]), std::ref(list_reduceOut[i]));
    }

    for (auto& el: threads) {
        el.join();
    }
}
