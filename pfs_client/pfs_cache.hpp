#pragma once

#include <cstdio>
#include <cstdlib>
#include <vector>
#include <string>
#include <queue>
#include <unordered_map>
#include <mutex>
#include <optional>
#include <iostream>
#include <ctime>
#include "pfs_common/pfs_config.hpp"
#include "pfs_api.hpp"




struct cacheBlock {
    std::string blockname;     
    std::string data; 
    bool dirty; 
    time_t last_access_time;
    int server_no;
    bool operator>(const cacheBlock& other) const {
        return last_access_time > other.last_access_time;
    }

};

struct Node {
    cacheBlock block; 
    Node* prev;       
    Node* next;       

    Node(const cacheBlock& blk) : block(blk), prev(nullptr), next(nullptr) {}
};
