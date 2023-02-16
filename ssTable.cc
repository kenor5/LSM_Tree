#include "./include/ssTable.h"
#include <iostream>

ssTable::ssTable(SkipList<uint64_t, std::string> &li)
{
    curNum = li.size();
    curSize = li.getSize();
    maxKey = li.getMax()->getKey();
    minKey = li.getMin()->getKey();
    li.scan(minKey, maxKey, data);
    uint64_t curOffset = 0;
    uint32_t hash[4] = {0};
    memset(bloom, '0', sizeof(bloom));
    for (auto it = data.begin(); it != data.end(); ++it) {
        offset.push_back(curOffset);
        curOffset += (*it).second.length();
        uint64_t key = (*it).first;
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(int i = 0; i < 4; ++i) bloom[hash[i]%10240] = '1';
    }
}

ssTable::ssTable() {

}

ssTable::~ssTable()
{
}

void ssTable::writeToFile(std::string fileName, uint64_t timeSteap) {
    std::fstream fout;
    fout.open(fileName, std::ios::out | std::ios::binary); 

    fout.write((char*)&timeSteap, sizeof(timeSteap));
    fout.write((char*)&curNum, sizeof(curNum));
    fout.write((char*)&minKey, sizeof(minKey));
    fout.write((char*)&maxKey, sizeof(maxKey));

    fout.write(bloom, sizeof(bloom));

    auto it2 = offset.begin();
    for (auto it = data.begin();it != data.end(); ++it,++it2) {
        fout.write((char*)(&(*it).first), sizeof((*it).first));
        fout.write((char*)(&(*it2)), sizeof(*it2));
    }

    for (auto it = data.begin();it != data.end(); ++it){
        char tmp[100 * KB];
        strcpy(tmp, (*it).second.c_str());
        fout.write((char*)tmp, (*it).second.length());
    }
    fout.close();
}

void ssTable::clear() {
    memset(bloom, '0', sizeof(bloom));
    data.clear();
    offset.clear();
}

std::string ssTable::readSstable(std::string fileName, uint32_t offset) {

	ifstream fin;
	fin.open(fileName, std::ios::in | std::ios::binary);

	if (!fin) {std::cout << "cannot open file!" << std::endl;}

	uint64_t timeSteap, maxKey, minKey, num, key;
	uint32_t off;
	char bloom[10 * KB] = "";

	fin.read((char *)&timeSteap, sizeof(timeSteap));
	fin.read((char *)&num, 		 sizeof(num));
	fin.read((char *)&minKey,    sizeof(minKey));
	fin.read((char *)&maxKey, 	 sizeof(maxKey));
	
	fin.read((char*)bloom, 10*KB);

	uint64_t i = 0;
	for (; i < num; ++i) {
		fin.read((char *)&key, sizeof(key));
		fin.read((char *)&off, sizeof(off));
		if (offset == off)
			break;
	}

	if (i != num) {
		fin.read((char *)&key, sizeof(key));
		fin.read((char *)&off, sizeof(off));
		off -= offset;
	}else off = 100*KB;

	char buf[100*KB];

	fin.seekg(32 + 10*KB + 12*num + offset);
	fin.read((char *)buf, off);
	// buf[off] = '\0';

	fin.close();
    string tmp = std::string(buf, buf+strlen(buf));

	return tmp;

}