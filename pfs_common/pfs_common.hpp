#pragma once

#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <sys/types.h>

#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <future>

#include "pfs_config.hpp"
using namespace std;



struct AcknowledgmentMapEntry {
    bool is_completed;
    pthread_cond_t* cv;
    string filename;
    int client_id;

};




std::string getMyHostname();
std::string getMyIP();
string generateUniqueID(string filename, int clientID, int token_type, int startOffset, int endOffset);
