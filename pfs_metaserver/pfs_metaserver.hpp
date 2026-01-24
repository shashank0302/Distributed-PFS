#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <cstring>
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

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include "pfs_common/pfs_common.hpp"

#include "pfs_proto/pfs_metaserver.pb.h"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"

struct FileDescriptor {
    int fd_id;
    int access_mode;
    int client_id;
    std::string filename;
    std::string unique_file_identifier;
    bool marked_for_deletion;
    pfsmeta::PfsMetadata *metadata;
};

struct Range {
    size_t start_offset;
    size_t end_offset;
    int mode;
    int client_id;
};



