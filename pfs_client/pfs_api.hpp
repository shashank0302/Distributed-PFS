#pragma once

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstdbool>
#include <vector>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <semaphore>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include<vector>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"

struct pfs_filerecipe {
    int stripe_width;
    int stripe_blocks;
    int pfs_block_size;


    // Additional...

};

struct Fileserverblock {
    string block_name;
    int server;
    uint64_t starting_bytes_of_block;
    uint64_t ending_bytes_of_block;
};

struct pfs_metadata {
    // Given metadata
    char filename[256];
    uint64_t file_size;
    time_t ctime;
    time_t mtime;
    int unique_id;
    string unique_file_identifier;
    std::vector<Fileserverblock> block_addresses; 
    struct pfs_filerecipe recipe;

    // Additional...

};

struct pfs_execstat {
    long num_read_hits;
    long num_write_hits;
    long num_evictions;
    long num_writebacks;
    long num_invalidations;
    long num_close_writebacks;
    long num_close_evictions;
};

struct FileServerEntry {
    std::string filename;      
    int server_no;             
    size_t starting_bytes_of_block;           
    size_t ending_bytes_of_block;             
    int start_block_offset;    
    int end_block_offset;
    size_t relative_start_position_from_offset;
    size_t relative_end_position_from_offset;
};

struct TokenRangeClient {
    size_t start_offset;
    size_t end_offset;
    int mode;
};

struct BusyTokenRange {
    size_t start_offset;
    size_t end_offset;
    bool is_busy;
    pthread_cond_t cv;
    int mode;
    bool operator<(const BusyTokenRange& other) const {
        return start_offset < other.start_offset;
    }
};

struct ThreadArgs {
    string filename;
    int64_t start_offset;
    int64_t end_offset;
    int token_type;
    int client_id;
};

struct filedescriptor{
    int fd;
    int access_mode;
};

int pfs_initialize();
int pfs_finish(int client_id);
int pfs_create(const char *filename, int stripe_width);
int pfs_open(const char *filename, int mode);
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset);
int pfs_close(int fd);
int pfs_delete(const char *filename);
int pfs_fstat(int fd, struct pfs_metadata *meta_data);
int pfs_execstat(struct pfs_execstat *execstat_data);
void writeback(string blockname,const string data,const int server,int pfs_block_size);
void invalidateCache(size_t start_offset,size_t end_offset,string filename,bool is_call_close);