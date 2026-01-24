#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#include <vector>
#include <random>
#include <mutex>

#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

std::mutex output_mutex; // Mutex for synchronized output

void read_thread(int fd, size_t num_bytes, off_t offset) {
    char *buf = (char *)malloc(num_bytes);
    memset(buf, 0, num_bytes);

    int ret = pfs_read(fd, buf, num_bytes, offset);
    if (ret == -1) {
        std::lock_guard<std::mutex> lock(output_mutex);
        fprintf(stderr, "Read error at offset %ld for %ld bytes.\n", offset, num_bytes);
    } else {
        std::lock_guard<std::mutex> lock(output_mutex);
        printf("Read %d bytes at offset %ld.\n", ret, offset);
    }

    free(buf);
}

void write_thread(int fd, size_t num_bytes, off_t offset) {
    char *buf = (char *)malloc(num_bytes);
    memset(buf, 'A' + (offset % 26), num_bytes); // Fill buffer with deterministic data

    int ret = pfs_write(fd, buf, num_bytes, offset);
    if (ret == -1) {
        std::lock_guard<std::mutex> lock(output_mutex);
        fprintf(stderr, "Write error at offset %ld for %ld bytes.\n", offset, num_bytes);
    } else {
        std::lock_guard<std::mutex> lock(output_mutex);
        printf("Wrote %d bytes at offset %ld.\n", ret, offset);
    }

    free(buf);
}

int main(int argc, char *argv[]) {
    printf("%s:%s: Start! Hostname: %s, IP: %s\n", __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Initialize the PFS client
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "pfs_initialize() failed.\n");
        return -1;
    }

    // Create a PFS file
    int ret = pfs_create("pfs_file2", 3);
    if (ret == -1) {
        fprintf(stderr, "Unable to create a PFS file.\n");
        return -1;
    }

    // Open the PFS file in write mode
    int pfs_fd = pfs_open("pfs_file2", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Error opening PFS file.\n");
        return -1;
    }

    // Write initial data to the file
    size_t initial_size = 1024 * 1024; // 1 MB
    char *init_buf = (char *)malloc(initial_size);
    memset(init_buf, 'A', initial_size);

    ret = pfs_write(pfs_fd, init_buf, initial_size, 0);
    if (ret == -1) {
        fprintf(stderr, "Write error during initialization.\n");
        return -1;
    } else {
        printf("Wrote %d bytes during initialization.\n", ret);
    }
    
    free(init_buf);

    // Launch 500 threads for concurrent access
    const int num_threads = 500;
    std::vector<std::thread> threads;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> size_dist(1024, 8192); // Random sizes between 1 KB and 8 KB
    std::uniform_int_distribution<off_t> offset_dist(0, initial_size - 8192); // Random offsets within file size

    for (int i = 0; i < num_threads; ++i) {
        size_t num_bytes = size_dist(gen);
        off_t offset = offset_dist(gen);

        if (i % 2 == 0) {
            // Read thread
            threads.emplace_back(read_thread, pfs_fd, num_bytes, offset);
        } else {
            // Write thread
            threads.emplace_back(write_thread, pfs_fd, num_bytes, offset);
        }
    }

    // Wait for all threads to finish
    for (auto &t : threads) {
        t.join();
    }

    // Close the PFS file
    ret = pfs_close(pfs_fd);
    if (ret == -1) {
        fprintf(stderr, "Error closing PFS file.\n");
        return -1;
    }

    // Print execution statistics
    struct pfs_execstat mystat = {0};
    ret = pfs_execstat(&mystat);
    
	if (ret != -1) {
		printf("Execution Statistics:\n");
		printf("num_read_hits: %ld\n", mystat.num_read_hits);
		printf("num_write_hits: %ld\n", mystat.num_write_hits);
		printf("num_evictions: %ld\n", mystat.num_evictions);
		printf("num_writebacks: %ld\n", mystat.num_writebacks);
		printf("num_invalidations: %ld\n", mystat.num_invalidations);
		printf("num_close_writebacks: %ld\n", mystat.num_close_writebacks);
		printf("num_close_evictions: %ld\n", mystat.num_close_evictions);
	}

	ret = pfs_finish(client_id);
	if (ret == -1) {
		fprintf(stderr, "pfs_finish() failed.\n");
		return -1;
	}

	printf("%s:%s: Finish!\n", __FILE__, __func__);
	return 0;
}
