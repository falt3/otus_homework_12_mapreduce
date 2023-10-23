#pragma once

#include <iostream>
#include <vector>
#include <deque>
#include <list>


class MapReduce {
public:
    MapReduce(const std::string& fnIn, size_t countMap, size_t countReduce) :
        countMap_(countMap),
        countReduce_(countReduce),
        filenameIn_(fnIn) {}

    void execute();

private:
    void split(const std::string& fn, std::vector<size_t> &list_pos);
    void map(std::vector<size_t>& list_pos, std::vector<std::list<std::string>> &list_map);
    void shuffle(std::vector<std::list<std::string>> &list_map, std::vector<std::list<std::string>> &list_reduce);
    void reduce(std::vector<std::list<std::string>> &list_reduceIn, std::vector<std::list<std::string>> &list_reduceOut);

    size_t countMap_;
    size_t countReduce_;
    std::string filenameIn_;
    std::string filenameOut_;
};