#include <iostream>
#include <string>
#include <fstream>
#include <filesystem>

#include "mapreduce.h"

namespace fs = std::filesystem;


int main(int argc, const char* argv[])
{
    std::string fn = "../tests/in2.txt";
    size_t countMap = 4;
    size_t countReduce = 1;
    
    if (argc == 4) {
        fn = argv[1];
        // countMap = argv[2];
        // countReduce = argv[3];
    }
    else if (argc != 1) {
        std::cout << "Неверное количество параметров\n";
        return 1;
    }

    fs::path path_fileIn(fn);
    if (fs::is_regular_file(path_fileIn)) 
        // dirs_in.push_back(fs::canonical(path).string());    
        std::cout << "file\n";
    else 
        std::cout << "not file\n";

    MapReduce mr(fn, countMap, countReduce);
    mr.execute();

    return 0;
}