#include "cache.h"

cache::cache(/* args */)
{
}

cache::~cache()
{
}


void cache::insert(ssTable &st, uint32_t l, uint32_t t,uint64_t s) {
    msg tmp(st, l, t, s);

    auto it = cacheList.begin();
    // while (it != cacheList.end())
    // {
    //     if ((*it).layer < l || ((*it).layer == l && (*it).th < t)){
    //         it++;
    //     }
    //     else break;
    // }
    cacheList.insert(it, tmp);
    
}

void cache::insert(msg m) {
    auto it = cacheList.begin();
    cacheList.insert(it, m);
}

bool cache::del(uint64_t timeSteap) {
    for (auto it = cacheList.begin(); it != cacheList.end(); ++it) {
        if ((*it).timeSteap == timeSteap){
            cacheList.erase(it);
            return 1;
        }
    }
    return 0;
}

layerThOff cache::search(uint64_t key) {
    uint32_t hash[4] = {0};
    // uint64_t start, end, mid;

    for (auto it = cacheList.begin(); it != cacheList.end(); ++it) {
        // 区间判断
        if (key > (*it).maxKey || key < (*it).minKey) continue;
        
        // bloomfilter判断是否不存在
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        if ((*it).bloom[hash[0]%10240] == '0') continue;
        if ((*it).bloom[hash[1]%10240] == '0') continue;
        if ((*it).bloom[hash[2]%10240] == '0') continue;
        if ((*it).bloom[hash[3]%10240] == '0') continue;

        // 二分查找 返回值为offset
        // start = 0;
        // end = (*it).num;
        // while (start <= end)
        // {
        //     mid = (start + end) / 2;
        //     uint64_t tmp = (*it).keyOffset[mid].first;
        //     if (tmp == key) return layerThOff((*it).layer, (*it).th, (*it).keyOffset[mid].second);
        //     if (tmp < key) start = mid + 1;
        //     else end = mid - 1;
        // }

        // 二分查找有点毛病，先用线性查找代替一下
        for (auto it1 = (*it).keyOffset.begin(); it1 != (*it).keyOffset.end(); it1++) {
            if ((*it1).first == key)
                return layerThOff((*it).layer, (*it).th, (*it1).second);
        }
        
    }

    return layerThOff(maxLayer, 0, 0);
}

void cache::clear() {
    cacheList.clear();
}
