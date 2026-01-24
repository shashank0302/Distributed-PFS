# Parallel File System (PFS)

## Overview

This project implements a **distributed parallel file system** using gRPC for communication between components. The system provides a POSIX-like interface for file operations while distributing data across multiple file servers for improved performance and fault tolerance.

## Architecture

The PFS consists of three main components:

### 1. **Meta Server** (`pfs_metaserver/`)
- **Purpose**: Central metadata management and coordination
- **Responsibilities**:
  - File metadata storage (size, timestamps, block locations)
  - File descriptor management
  - Token-based concurrency control for read/write operations
  - Client registration and session management
  - File creation, deletion, and access control

### 2. **File Servers** (`pfs_fileserver/`)
- **Purpose**: Distributed data storage
- **Configuration**: 4 file servers (configurable via `pfs_config.hpp`)
- **Responsibilities**:
  - Block-level data storage and retrieval
  - Write/read operations on file blocks
  - Data persistence and integrity

### 3. **PFS Client** (`pfs_client/`)
- **Purpose**: Client-side interface and caching
- **Features**:
  - POSIX-like API (`pfs_open`, `pfs_read`, `pfs_write`, `pfs_close`, etc.)
  - LRU cache implementation (16 blocks by default)
  - Write-back caching with dirty bit tracking
  - Token-based concurrency control

## Key Features

### **Data Striping**
- **Block Size**: 512 bytes (configurable)
- **Stripe Width**: Configurable per file (default: 3)
- **Stripe Blocks**: 2 blocks per stripe
- **Distribution**: Data blocks distributed across multiple file servers

### **Concurrency Control**
- **Token System**: Prevents read/write conflicts
- **Range-based Locking**: Granular control over file regions
- **Client Isolation**: Multiple clients can access different file regions simultaneously

### **Caching**
- **LRU Eviction**: Least Recently Used block replacement
- **Write-back**: Delayed writes for performance
- **Cache Statistics**: Hit/miss tracking and performance metrics

## API Interface

The PFS provides a POSIX-like interface:

```c
int pfs_initialize();                                    // Initialize PFS client
int pfs_finish(int client_id);                          // Cleanup and disconnect
int pfs_create(const char *filename, int stripe_width); // Create new file
int pfs_open(const char *filename, int mode);           // Open file (READ_MODE=1, WRITE_MODE=2)
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);   // Read data
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset); // Write data
int pfs_close(int fd);                                  // Close file descriptor
int pfs_delete(const char *filename);                   // Delete file
int pfs_fstat(int fd, struct pfs_metadata *meta_data); // Get file statistics
int pfs_execstat(struct pfs_execstat *execstat_data);   // Get cache statistics
```

## Communication Protocol

### **gRPC Services**
- **Meta Server Protocol** (`pfs_metaserver.proto`): Client-meta server communication
- **File Server Protocol** (`pfs_fileserver.proto`): Client-file server communication

### **Key RPC Methods**
- `initializeClient`: Client registration
- `createFile`/`openFile`: File management
- `writeFile`/`readFile`: Data operations
- `TokenStream`: Bidirectional streaming for concurrency control

## Build System

### **Dependencies**
- **Compiler**: GCC 11.3.0
- **gRPC**: Custom installation at `/home/other/CSE511-FA24/grpc`
- **Protocol Buffers**: For serialization
- **Threading**: POSIX threads for concurrency

### **Build Commands**
```bash
make                    # Build all components
make clean             # Clean build artifacts
make pfs_proto         # Generate protocol buffer code
make pfs_common        # Build common utilities
make pfs_metaserver    # Build meta server
make pfs_fileserver    # Build file servers
make pfs_client        # Build client library
```

## Deployment

### **Server Configuration**
- Edit `pfs_list.txt` to specify server hostnames and ports
- First server in list runs the meta server
- Remaining servers run file servers

### **Launch Script**
```bash
./launcher.sh 0    # Launch appropriate server based on hostname
```

## Testing

The project includes comprehensive test clients in `client-1/`:
- **Basic Operations**: File create, read, write, close
- **Concurrency Testing**: Multiple client access patterns
- **Cache Testing**: LRU eviction and write-back behavior
- **Verification**: Data integrity and consistency checks

## Configuration

Key parameters in `pfs_config.hpp`:
- `PFS_BLOCK_SIZE`: 512 bytes
- `NUM_FILE_SERVERS`: 4 servers
- `STRIPE_BLOCKS`: 2 blocks per stripe
- `CLIENT_CACHE_BLOCKS`: 16 cache blocks

## Project Structure

```
p2-pfs-team-curious-1/
├── pfs_proto/          # Protocol buffer definitions
├── pfs_common/         # Shared utilities and configuration
├── pfs_metaserver/     # Meta server implementation
├── pfs_fileserver/     # File server implementation
├── pfs_client/         # Client library and API
├── client-1/           # Test clients and examples
├── launcher.sh         # Server deployment script
├── pfs_list.txt        # Server configuration
└── Makefile           # Build system
```


This project is part of **PSU CSE 511 Fall 2024** and demonstrates concepts in:
- Distributed systems design
- File system implementation
- Concurrency control
- Caching strategies
- Network programming with gRPC
- Protocol buffer serialization
