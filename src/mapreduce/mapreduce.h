#pragma once

#include <iostream>
#include <vector>
#include <deque>
#include <list>
#include <functional>
#include <fstream>


struct IReader {
    virtual std::string getData() = 0;
};

struct ReaderFile : public IReader {
    ReaderFile(const std::string filename, size_t startPos, size_t endPos) :
        endPos_(endPos) 
    {
        ifile_.open(filename);
        ifile_.seekg(startPos);
    };
    // ReaderFile(ReaderFile&& other) {}
    std::string getData() override {
        std::string str;
        while (ifile_.tellg() < endPos_ - 1) {
            if (std::getline(ifile_, str) && !str.empty()) 
                return str;
        }
        return str;
    };
private:
    std::ifstream ifile_;
    size_t endPos_;    
};



class MapReduce {
public:
    using TypeList = std::deque<std::string>;

    using Func_map = std::function<void(IReader& reader, TypeList&)>;
    using Func_reduce = std::function<void(TypeList& in, TypeList& out)>;

    static void execute(const std::string& filenameIn, const std::string& filenameOut, 
                size_t countMaps, size_t countReduces, 
                Func_map func_map, Func_reduce func_reduce);
};