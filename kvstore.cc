#include "./include/kvstore.h"
#include <iostream>
#include <algorithm>
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
	memTable = new SkipList<uint64_t, std::string> (UINT64_MAX);
	mergeTable = new SkipList<uint64_t, std::string> (UINT64_MAX);
	curTime = 1;

	init();
}

void KVStore::init() {
	string dirName, fileName;
	vector<string> files;
	ifstream fin;
	for (int i = 0; i <= maxLayer; ++i) {
		dirName = getDirName(i);
		if (!utils::dirExists(dirName)) break;
		curLayer = i;
		
		files.clear();
		utils::scanDir(dirName, files);
		sort(files.begin(), files.end()); // 排序完成就是按照时间戳小到大
		
		// load cache
		for (auto it = files.begin(); it != files.end(); ++it) {
			fileName = dirName + "/" + (*it);
			fin.open(fileName, ios::in);
			if (!fin) 
			cerr<<"cannot open it!" <<endl;
			
			uint64_t timeSteap, maxKey, minKey, num, key;
			uint32_t off;
			msg tmpMsg;
			tmpMsg.layer = i;
			tmpMsg.th = stoi((*it).substr(4, (*it).find(".sst") - 4));
			layer[i] = layer[i] > tmpMsg.th ? layer[i] : tmpMsg.th;
			char bloom[10 * KB] = "";

			fin.read((char *)&timeSteap, sizeof(timeSteap));
			fin.read((char *)&num, 		 sizeof(num));
			fin.read((char *)&minKey,    sizeof(minKey));
			fin.read((char *)&maxKey, 	 sizeof(maxKey));
			curTime = curTime > timeSteap ? curTime : timeSteap;
			tmpMsg.timeSteap = timeSteap;
			tmpMsg.num = num;
			tmpMsg.minKey = minKey;
			tmpMsg.maxKey = maxKey;
			
			fin.read((char*)bloom, 10*KB);
			strcpy(tmpMsg.bloom, bloom);
			
			for (uint64_t j = 0; j < num; ++j) {
				fin.read((char *)&key, sizeof(key));
				fin.read((char *)&off, sizeof(off));
				tmpMsg.keyOffset.push_back(pair<uint64_t, uint32_t>(key, off));
			}
			caches[i].insert(tmpMsg);
			fin.close();
		}

	}

	fin.open("./data/log/wal.log", ios::in);
	// 存在wal文件
	if (fin.is_open()) {
		int size;
		uint64_t key;
		char buf[100*KB];
		string str;
		while (fin.peek() != EOF) {
			fin.read((char*)&key, sizeof(key));
			fin.read((char*)&size, sizeof(size));
			memset(buf, '\0', sizeof(buf));
			fin.read((char *)buf, size);
			str = string(buf, buf+strlen(buf));
			this->put(key, str);
		}
		fin.close();
		clearWal();
	}
}

KVStore::~KVStore()
{
	if (memTable->size() > 0) {
		ssTable ss(*memTable);
		caches[0].insert(ss, 0, layer[0], curTime);
		std::string fileName = getFileName(0, layer[0]);
		if (!utils::dirExists(getDirName(0))) {
			utils::mkdir(getDirName(0).c_str());
		}
		ss.writeToFile(fileName, curTime);
		layer[0] ++;
		curTime++;

		compaction();

		memTable->clear();
		clearWal();
	}

	delete memTable;
	delete mergeTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	// not oversized after insertion
	if (memTable->getSize() + 12 + s.length() < 2*MB){
		writeToWal(key, s);
		memTable->insert(key, s);
	}
	else {
		// Node<uint64_t, std::string> *tmp = memTable->search(key);
		// if (tmp != nullptr && memTable->getSize() - tmp->getValue().length() + s.length() < 2*MB) {
		// 	memTable->insert(key, s);
		// 	return;
		// }

		ssTable ss(*memTable);
		caches[0].insert(ss, 0, layer[0], curTime);	// insert to cache
		std::string fileName = getFileName(0, layer[0]);
		if (!utils::dirExists(getDirName(0))) {
			utils::mkdir(getDirName(0).c_str());
		}
		ss.writeToFile(fileName, curTime);
		layer[0] ++;
		curTime++;

		compaction();

		memTable->clear();
		clearWal();
		memTable->insert(key, s);
		writeToWal(key, s);
	}


}

void KVStore::writeToWal(uint64_t key, const std::string& s) {
	std::string logFolder = "./log";
	if (!utils::dirExists(logFolder)) {
		std::cerr << "not exit wal";
			std::cerr << utils::mkdir(logFolder.c_str());

		}
	
	std::string walName = logFolder + "/wal.log";
	std::ofstream fout(walName,std::ios::out | std::ios::app | std::ios::binary);
	if (!fout.is_open() ) std::cerr << "fail to open wal file \n";
	else {
		int size = s.size();
		fout.write((char*)&key, sizeof(key));
		fout.write((char*)&size, sizeof(size));
		fout.write(s.c_str(), size);
		fout.close();
	}
}

void KVStore::clearWal() {
	char walName[] = "./log/wal.log";
	utils::rmfile(walName);
}


// put to a spicific layer, called by merge func
void KVStore::putToLayer(uint64_t k, const std::string &s, int l, uint64_t t) {

	if (mergeTable->getSize() + 12 + s.length() < 2*MB)
		mergeTable->insert(k, s);
	else {
		ssTable ss(*mergeTable);
		caches[l].insert(ss, l, layer[l], t);	// insert to cache
		std::string fileName = getFileName(l, layer[l]);
		if (!utils::dirExists(getDirName(l))){
			utils::mkdir(getDirName(l).c_str());
			curLayer = l;
		}
		ss.writeToFile(fileName, curTime);
		layer[l] ++;

		mergeTable->clear();
		mergeTable->insert(k, s);
	}
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	Node<uint64_t, std::string> *cur = memTable->search(key);
	if (cur && cur->getValue() == "~DELETED~") return "";
	if (cur) return cur->getValue();

	layerThOff offset;
	for (uint64_t i = 0; i <= curLayer; ++i) {
		offset = caches[i].search(key);
		if (offset.layer != maxLayer)
			break;
	}
	
	if (offset.layer == maxLayer) // not exist in sstable
		return "";
	

	string fileName = getFileName(offset.layer, offset.th);
	string rt = readSstable(fileName, offset.offset);
	

	if (rt == "~DELETED~") return "";

	return rt;

}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	if (get(key) == "") return false;
	auto tmp = memTable->search(key);
	if (tmp && tmp->getValue() == "~DELETED~") return true;
	memTable->remove(key);
    put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	clearWal();
	memTable->clear();

	vector<string> fileName;
	for (int i = 0; i<= maxLayer; ++i) {
		string dirName = getDirName(i);
		fileName.clear();
		if (utils::dirExists(dirName)) {
			utils::scanDir(dirName, fileName);
			for (auto it = fileName.begin(); it != fileName.end(); ++it) {
				string file = dirName + "/" + (*it);
				utils::rmfile(file.c_str());
			}
			utils::rmdir(dirName.c_str());
		}else break;
	
	}

	memset(layer, 0, sizeof(layer));
	curTime = 1;
	curLayer = 0;
	for (int i = 0; i < maxLayer; ++i)
		caches[i].clear();
}

bool cmp(pair<uint64_t, std::string>a, pair<uint64_t, std::string>b) { //merge辅助函数
	return a.first < b.first;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
	string fileName;
	memTable->scan(key1, key2, list);

	for (int i = 0; i <= curLayer; ++i) {
		for (auto it = caches[i].cacheList.begin(); it != caches[i].cacheList.end(); it++) {
			if (it->minKey > key1 && it->maxKey < key2) continue;
			fileName = getFileName(i, (*it).th);

			std::list<std::pair<uint64_t, std::string>> curList = scanSstable(fileName, key1, key2);

			if(curList.empty()) continue;

			list.merge(curList, cmp);
		}
	}
}



std::string KVStore::getFileName(int layer, int num) {
	return "./data/level-" + to_string(layer) + "/set_" + to_string(num) + ".sst";
}

std::string KVStore::getDirName(int layer) {
	return "./data/level-" + to_string(layer) ;
}

bool KVStore::layerOverSized(int l) {
	return caches[l].cacheList.size() > (uint64_t)(2<<l);
}

std::string KVStore::readSstable(std::string fileName, uint32_t offset) {

	ifstream fin;
	fin.open(fileName, std::ios::in | std::ios::binary);

	if (!fin) {
		std::cerr << "cannot open file!" << std::endl;
		std::cerr << fileName;
		}
	

	uint64_t timeSteap, maxKey, minKey, num, key;
	uint32_t off;
	char bloom[10 * KB] = "";

	fin.read((char *)&timeSteap, sizeof(timeSteap));
	fin.read((char *)&num, 		 sizeof(num));
	fin.read((char *)&minKey,    sizeof(minKey));
	fin.read((char *)&maxKey, 	 sizeof(maxKey));
	
	fin.read((char*)bloom, 10*KB);

	int i = 0;
	for (; i < num; ++i) {
		fin.read((char *)&key, sizeof(key));
		fin.read((char *)&off, sizeof(off));
		if (offset == off)
			break;
	}

	if (i != num-1) {
		fin.read((char *)&key, sizeof(key));
		fin.read((char *)&off, sizeof(off));

		off -= offset;
	}else off = 100*KB;


	char buf[100*KB];
	memset(buf, '\0', sizeof(buf));
	fin.seekg(32 + 10*KB + 12*num + offset);
	fin.read((char *)buf, off);

	fin.close();

	string tmp =  std::string(buf, buf+strlen(buf));

	return tmp;
}

std::list<std::pair<uint64_t, std::string>> KVStore::scanSstable(std::string fileName, uint64_t key1, uint64_t key2) {

	std::list<std::pair<uint64_t, std::string>> rt;

	std::list<std::pair<uint64_t, uint32_t>> keyOff;
	ifstream fin;
	fin.open(fileName, std::ios::in | std::ios::binary);

	if (!fin) {std::cerr << "cannot open file! scan" << std::endl;}

	uint64_t timeStamp, maxKey, minKey, num, key;
	uint32_t off;
	char bloom[10 * KB] = "";

	fin.read((char *)&timeStamp, sizeof(timeStamp));
	fin.read((char *)&num, 		 sizeof(num));
	fin.read((char *)&minKey,    sizeof(minKey));
	fin.read((char *)&maxKey, 	 sizeof(maxKey));
	
	if (key1 > maxKey || key2 < minKey) {rt.clear(); return rt;}  // not in range

	fin.read((char*)bloom, 10*KB);

	for (int i = 0; i < num; ++i) {
		fin.read((char *)&key, sizeof(key));
		fin.read((char *)&off, sizeof(off));
		keyOff.push_back(std::pair<uint64_t, uint32_t> (key, off));
	}

	auto it1 = keyOff.begin();
	for (; it1 != keyOff.end() && key1 > (*it1).first; it1++){}

	fin.seekg((*it1).second, ios::cur);

	char buf[100*KB];
	uint64_t curOff, nextOff, curKey;
	for (;it1 != keyOff.end() && key2 >= (*it1).first; it1++){
		curKey = (*it1).first;
		curOff = (*it1).second;
		it1++;
		if (it1 == keyOff.end()) 
		{
			int prePos = fin.tellg();
			fin.seekg(0, ios::end);
			int tmp = fin.tellg();
			uint64_t tmp1 = 10272U + num*12U;
			nextOff = tmp - tmp1;
			fin.seekg(prePos);
		}
		else nextOff = (*it1).second;
		it1 --;

		nextOff -= curOff;
		fin.read(buf, nextOff);
		rt.push_back(std::pair<uint64_t, std::string> (curKey, std::string(buf, buf+nextOff)));

	}

	fin.close();
	return rt;

}


std::list<keyValTime> KVStore::dumpAll(string fileName) {
	std::list<keyValTime> rt;

	std::list<std::pair<uint64_t, uint32_t>> keyOff;
	ifstream fin;
	fin.open(fileName, std::ios::in | std::ios::binary);

	if (!fin) {
		std::cerr << "cannot open file! dump" << std::endl;
		}

	uint64_t timeStamp, maxKey, minKey, num, key;
	uint32_t off;
	char bloom[10 * KB] = "";

	fin.read((char *)&timeStamp, sizeof(timeStamp));
	fin.read((char *)&num, 		 sizeof(num));
	fin.read((char *)&minKey,    sizeof(minKey));
	fin.read((char *)&maxKey, 	 sizeof(maxKey));

	fin.read((char*)bloom, 10*KB);

	for (int i = 0; i < num; ++i) {
		fin.read((char *)&key, sizeof(key));
		fin.read((char *)&off, sizeof(off));
		keyOff.push_back(std::pair<uint64_t, uint32_t> (key, off));
	}

	

	char buf[100*KB];
	uint64_t curOff, nextOff, curKey;
	for (auto it1 = keyOff.begin(); it1 != keyOff.end(); it1++){
		curKey = (*it1).first;
		curOff = (*it1).second;
		it1++;
		if (it1 == keyOff.end()) 
		{
			int prePos = fin.tellg();
			fin.seekg(0, ios::end);
			int tmp = fin.tellg();
			uint64_t tmp1 = 10272U + num*12U;
			nextOff = tmp - tmp1;
			fin.seekg(prePos);
		}
		else nextOff = (*it1).second;
		it1 --;

		nextOff -= curOff;
		fin.read(buf, nextOff);
		rt.push_back(keyValTime(curKey, std::string(buf, buf+nextOff), timeStamp));

	}

	fin.close();
	return rt;
}

void KVStore::compaction() {
	int l = 0;
	if (layerOverSized(l)) {
		mergeFirstLayer();
	}else return;

	l++;
	while (layerOverSized(l))
	{
		mergeOtherLayer(l);
		l++;
	}
	
}

// func to help merge keyValTime
bool cmp2(keyValTime a, keyValTime b) {
	if (a.key == b.key) return a.stamp > b.stamp;
	return a.key < b.key;
}

void KVStore::mergeFirstLayer() {
	uint64_t maxTimeStamp = 0, minKey = __LONG_MAX__, maxKey = 0, tmp;
	vector<string> fileToMerge;
	bool lastLayer = !(utils::dirExists("./data/level-0"));

	fileToMerge.clear();

	for (auto it = caches[0].cacheList.begin(); it != caches[0].cacheList.end(); it++) {
		string tmp1 = getFileName((*it).layer, (*it).th);
		fileToMerge.push_back(tmp1);

		tmp = (*it).timeSteap;
		maxTimeStamp = maxTimeStamp > tmp ? maxTimeStamp : tmp;   // store the max timestamp
		tmp = (*it).minKey;
		minKey = minKey < tmp ? minKey : tmp;
		tmp = (*it).maxKey;
		maxKey = maxKey > tmp ? maxKey : tmp;
	}
	caches[0].clear();   // 删除缓存

	for (auto it = caches[1].cacheList.begin(); it != caches[1].cacheList.end();) {
		tmp = (*it).minKey;
		uint64_t tmp2 = (*it).maxKey;

		if (tmp > maxKey || tmp2 < minKey){ ++it; continue;}

		string tmp1 = getFileName((*it).layer, (*it).th);
		fileToMerge.push_back(tmp1);

		tmp = (*it).timeSteap;
		maxTimeStamp = maxTimeStamp > tmp ? maxTimeStamp : tmp;  // store the max timestamp

		it = caches[1].cacheList.erase(it); // 删除缓存
	}

	list<keyValTime> all;
	all.clear();

	for (int i = 0; i < fileToMerge.size(); ++i) {
		list<keyValTime> tmp = dumpAll(fileToMerge[i]);
		all.merge(tmp, cmp2);

		utils::rmfile(fileToMerge[i].c_str());
	}

	mergeTable->clear();
	uint64_t preKey = UINT64_MAX;
	for (auto it = all.begin(); it != all.end(); ++it) {
		if ((*it).key != preKey){
			if (!(lastLayer && (*it).val == "~DELETED~"))
				putToLayer((*it).key, (*it).val, 1, maxTimeStamp);
			preKey = (*it).key;
		}
	}

	if (mergeTable->size() > 0) {
		if (lastLayer)
			mergeTable->removeDelete();
		ssTable ss(*mergeTable);
		caches[1].insert(ss, 1, layer[1], maxTimeStamp);	// insert to cache
		std::string fileName = getFileName(1, layer[1]);
		if (!utils::dirExists(getDirName(1))){
			utils::mkdir(getDirName(1).c_str());
			curLayer = 1;
			}
		ss.writeToFile(fileName, maxTimeStamp);
		layer[1] ++;

		mergeTable->clear();
	}

	layer[0] = 0;
	
}

void KVStore::mergeOtherLayer(int l) {

	uint64_t maxTimeStamp = 0, minKey = __LONG_MAX__, maxKey = 0, tmp;
	vector<string> fileToMerge;
	bool lastLayer = 0;

	fileToMerge.clear();

	auto it = caches[l].cacheList.begin() + (2 << l);
	for (; it != caches[l].cacheList.end(); ) {
		string tmp1 = getFileName((*it).layer, (*it).th);
		fileToMerge.push_back(tmp1);

		tmp = (*it).timeSteap;
		maxTimeStamp = maxTimeStamp > tmp ? maxTimeStamp : tmp;   // store the max timestamp
		tmp = (*it).minKey;
		minKey = minKey < tmp ? minKey : tmp;
		tmp = (*it).maxKey;
		maxKey = maxKey > tmp ? maxKey : tmp;

		it = caches[l].cacheList.erase(it); // remove the cache, and move the pointer to next element
	}

	for (it = caches[l+1].cacheList.begin(); it != caches[l+1].cacheList.end();) {
		tmp = (*it).minKey;
		uint64_t tmp2 = (*it).maxKey;

		if (tmp > maxKey || tmp2 < minKey){ ++it; continue;}

		string tmp1 = getFileName((*it).layer, (*it).th);
		fileToMerge.push_back(tmp1);

		tmp = (*it).timeSteap;
		maxTimeStamp = maxTimeStamp > tmp ? maxTimeStamp : tmp;  // store the max timestamp

		it = caches[l+1].cacheList.erase(it); // 删除缓存
	}

	list<keyValTime> all;
	all.clear();

	for (int i = 0; i < fileToMerge.size(); ++i) {
		list<keyValTime> tmp = dumpAll(fileToMerge[i]);
		all.merge(tmp, cmp2);

		utils::rmfile(fileToMerge[i].c_str());
	}

	if (!utils::dirExists(getDirName(l+1))) lastLayer = 1;

	mergeTable->clear();
	uint64_t preKey = UINT64_MAX;
	for (auto it = all.begin(); it != all.end(); ++it) {
		if ((*it).key != preKey){
			if (!(lastLayer && (*it).val == "~DELETED~"))
				putToLayer((*it).key, (*it).val, l+1, maxTimeStamp);
			preKey = (*it).key;
		}
	}

	if (mergeTable->size() > 0) {
		if (lastLayer) 
			mergeTable->removeDelete();

		ssTable ss(*mergeTable);
		caches[l+1].insert(ss, l+1, layer[l+1], maxTimeStamp);	// insert to cache
		std::string fileName = getFileName(l+1, layer[l+1]);
		if (!utils::dirExists(getDirName(l+1))){
			utils::mkdir(getDirName(l+1).c_str());
			curLayer = l + 1;
			}
		ss.writeToFile(fileName, maxTimeStamp);
		layer[l+1] ++;

		mergeTable->clear();
	}

}