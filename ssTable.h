#ifndef _SSTABLE_H_
#define _SSTABLE_H_


#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include <list>
#include <fstream>

#include "const.h"
#include "skiplist.cc"
#include "MurmurHash3.h"


class ssTable
{

public:
    uint64_t curSize, curNum;
    uint64_t maxKey, minKey;
    list<pair<uint64_t, std::string>> data;
    list<uint32_t> offset;
    char bloom[10 * KB];

    void setOffset();

    ssTable(SkipList<uint64_t, std::string> &li);
    ssTable();
    ~ssTable();

    void clear();
    void writeToFile(std::string fileName, uint64_t timeSteap);
    std::string readSstable(std::string fileName, uint32_t offset);
    
};


#endif