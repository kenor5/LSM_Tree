#ifndef SKIPLISTPRO_SKIPLIST_H
#define SKIPLISTPRO_SKIPLIST_H

#include <cstddef>
#include <cassert>
#include <ctime>
#include <list>
#include "Node.h"
#include "random.h"
#include "const.h"

using namespace std;

#define DEBUG


template<typename K, typename V>
class SkipList {

public:
    SkipList(K footerKey) : rnd(0x12345678) {
        createList(footerKey);
    }

    ~SkipList() {
        freeList();
    }

    //注意:这里要声明成Node<K,V>而不是Node,否则编译器会把它当成普通的类
    Node<K, V> *search(K key) const;

    bool insert(K key, V value);

    bool remove(K key);

    void scan(K key1, K key2, list<pair<K, V> > &li);

    int size() {
        return nodeCount;
    }

    int getLevel() {
        return level;
    }

    uint64_t getSize() {
        return curSize;
    }
    Node<K, V> * getMin();

    Node<K, V> * getMax();

    void clear();

    void removeDelete();

private:
    uint64_t curSize;
    //初始化表
    void createList(K footerKey);

    //释放表
    void freeList();

    //创建一个新的结点，节点的层数为level
    void createNode(int level, Node<K, V> *&node);

    void createNode(int level, Node<K, V> *&node, K key, V value);

    //随机生成一个level
    int getRandomLevel();


private:
    int level;
    Node<K, V> *header;
    Node<K, V> *footer;

    size_t nodeCount;

    static const int MAX_LEVEL = 16;

    Random rnd;
};

#endif