#include "kvstore.cc"
#include <ctime>

int main () {

    KVStore store("./data");
    store.reset();
    time_t start, end;
    const uint64_t maxVal = 1024*2;
    uint64_t cnt = 1;
    for (uint32_t i = 1; i <= 100; ++i) {
        uint64_t val = i*maxVal;
        list<pair<uint64_t, string>> tmp;
        start = clock();
        for (uint64_t j = 0; j <= 400; ++j) {
            store.put(cnt, string(500, 's'));
            cnt++;
        }
        end = clock();
        cout <<  0.1/((double)(end-start)/CLOCKS_PER_SEC*1000/val) << endl;

        // start = clock();
        // for (uint64_t j = 1; j <= val; ++j) {
        //     store.get(j);
        // }
        // end = clock();
        // cout << val << " get" << (double)(end-start)/CLOCKS_PER_SEC*1000/val << endl;

        // tmp.clear();
        // start = clock();
        // for (uint64_t j = 1; j <= val; ++j) {
        //     store.scan(0, 100, tmp);
        // }
        // end = clock();
        // cout << val << " scan" << (double)(end-start)/CLOCKS_PER_SEC*1000/val << endl;

        // start = clock();
        // for (uint64_t j = 1; j <= val; ++j) {
        //     store.del(j);
        // }
        // end = clock();
        // cout << val << " del" << (double)(end-start)/CLOCKS_PER_SEC*1000/val << endl;
        // store.reset();
    }


    return 0;
}
