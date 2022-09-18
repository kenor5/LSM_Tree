#ifndef _CACHE_H_
#define _CACHE_H_

#include <vector>
#include <cstdint>
#include <list>
#include <algorithm>
#include "ssTable.h"
// #include "const.h"
#include "MurmurHash3.h"


struct layerThOff{
    uint32_t layer, th;
    uint32_t offset;

    layerThOff(uint32_t l, uint32_t t, uint32_t off) {
        layer = l;
        th = t;
        offset = off;
    }

    layerThOff(){

    }
};

struct msg{
    uint32_t layer, th;
    uint64_t timeSteap, minKey, maxKey, num;
    char bloom[10*KB];
    std::list<std::pair<uint64_t, uint32_t>> keyOffset;
    msg(ssTable &st, uint32_t l, uint32_t t, uint64_t s) {
        layer = l;
        th = t;
        timeSteap = s;
        minKey = st.minKey;
        maxKey = st.maxKey;
        num = st.curNum;
        strcpy(bloom, st.bloom);
        auto it1 = st.data.begin();
        for (auto it2 = st.offset.begin(); it2 != st.offset.end() && it1 != st.data.end(); it1++, it2++) {
            keyOffset.push_back(std::pair<uint64_t, uint32_t>((*it1).first, *it2));
        }
    }

    msg() {}
    ~msg() {}
};

class cache
{
public:
    std::vector<msg> cacheList;

    cache(/* args */);
    ~cache();
    void insert(ssTable &st, uint32_t, uint32_t, uint64_t);
    void insert(msg m);
    bool del(uint64_t timeSteap);
    layerThOff search(uint64_t);
    void clear();
};



#endif