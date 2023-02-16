#ifndef _KVSTORE_H_
#define	_KVSTORE_H_

#include "kvstore_api.h"
#include "skiplist.h"
#include "ssTable.h"
#include "utils.h"
#include "cache.h"


class keyValTime{
public:
    uint64_t key = 0, stamp = 0;
    string val = "";
    keyValTime(uint64_t Key, string Val, uint64_t Stamp):key(Key),val(Val), stamp(Stamp){}
    keyValTime(){};
};



class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	uint32_t layer[maxLayer] = {0};
	uint32_t curLayer = 0;
	SkipList<uint64_t, std::string> *memTable;
	SkipList<uint64_t, std::string> *mergeTable; //skiplist used to merge
	uint64_t curTime;
	cache caches[maxLayer];
	std::string dataFolder = "./data";

	std::string getFileName(int layer, int num);

	std::string getDirName(int layer);

	bool layerOverSized(int l);

	void compaction();

	void mergeFirstLayer();

	void mergeOtherLayer(int);

	void writeToWal(uint64_t, const std::string&);

	void clearWal();

	std::string readSstable(std::string, uint32_t);

	std::list<std::pair<uint64_t, std::string>> scanSstable(string, uint64_t, uint64_t);

	std::list<keyValTime> dumpAll(string);

	void putToLayer(uint64_t key, const std::string &s, int layer, uint64_t time);

	void init();
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;


};

#endif