#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

int main(int argc, char *argv[]) {
    printf("%s:%s: Start! Hostname: %s, IP: %s\n", __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    if (argc < 2) {
        fprintf(stderr, "%s: usage: ./sample1-1 <input filename>\n", __func__);
        return -1;
    }

    // Open a file
    std::string input_filename(argv[1]);
    int input_fd = open(input_filename.c_str(), O_RDONLY);
    char *buf = (char *)malloc(8 * 1024);  // Buffer for writing
    char *buf1 = (char *)malloc(8 * 1024); // Buffer for reading
    char *buf2 = (char *)malloc(8 * 1024); // Buffer for reading

   

    ssize_t nread = pread(input_fd, (void *)buf, 1024, 0);
    if (nread != 1024) {
        fprintf(stderr, "pread() error.\n");
        return -1;
    }
    close(input_fd);

    // Initialize the PFS client
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "pfs_initialize() failed.\n");
        return -1;
    }

    // // Create a PFS file
    int ret;
    // ret = pfs_create("pfs_file1", 3);
    // if (ret == -1) {
    //     fprintf(stderr, "Unable to create a PFS file.\n");
    //     return -1;
    // }

    // Open the PFS file in write mode
    int pfs_fd = pfs_open("pfs_file1", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Error opening PFS file.\n");
        return -1;
    }
     ret = pfs_read(pfs_fd, (void *)buf2, 800, 0);
    if (ret == -1) {
        fprintf(stderr, "Read error from PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read %d bytes from the PFS file from offset 0 to 800.\n", __FILE__, __func__, ret);

        int count = 0;
        for (int i = 0; i < ret; i++) {
            if ((i <= 612 && buf[i] == buf2[i]) || (i > 612 && buf2[i] == '\0')) { 
                count++;
            }
        }
        printf("Read verification count from buf2: %d\n", count);
    }

    // Step 1: Write 512 bytes to pfs_file1 at offset 0
    ret = pfs_write(pfs_fd, (void *)buf, 512, 0);
    if (ret == -1) {
        fprintf(stderr, "Write error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Wrote %d bytes to the PFS file at offset 0.\n", __FILE__, __func__, ret);
    }

    // Step 2: Read back those 512 bytes and verify
    ret = pfs_read(pfs_fd, (void *)buf1, 512, 0);
    if (ret == -1) {
        fprintf(stderr, "Read error from PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read %d bytes from the PFS file at offset 0.\n", __FILE__, __func__, ret);

        int count = 0;
        for (int i = 0; i < 512; i++) {
            if (buf[i] == buf1[i]) {
                count++;
            }
        }
        printf("Read verification count from buf1: %d\n", count);
    }

    // Step 3: Write another 100 bytes starting at offset 513
    ret = pfs_write(pfs_fd, (void *)(buf + 513), 100, 513);
    if (ret == -1) {
        fprintf(stderr, "Write error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Wrote %d bytes to the PFS file at offset 513.\n", __FILE__, __func__, ret);
    }

    // Step 4: Read from offset 0 to 800 and verify
    ret = pfs_read(pfs_fd, (void *)buf2, 800, 0);
    if (ret == -1) {
        fprintf(stderr, "Read error from PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read %d bytes from the PFS file from offset 0 to 800.\n", __FILE__, __func__, ret);

        int count = 0;
        for (int i = 0; i < ret; i++) {
            if ((i <= 612 && buf[i] == buf2[i]) || (i > 612 && buf2[i] == '\0')) { 
                count++;
            }
        }
        printf("Read verification count from buf2: %d\n", count);
    }

    // Close the PFS file
    ret = pfs_close(pfs_fd);
    if (ret == -1) {
        fprintf(stderr, "Error closing PFS file.\n");
        return -1;
    }

    // Delete the PFS file
    // ret = pfs_delete("pfs_file1");
    // if (ret == -1) {
    //     fprintf(stderr, "pfs_delete() failed.\n");
    //     return -1;
    // }

    // Finish the PFS client
    ret = pfs_finish(client_id);
    if (ret == -1) {
        fprintf(stderr, "pfs_finish() failed.\n");
        return -1;
    }

    free(buf);
    free(buf1);
    free(buf2);

    printf("%s:%s: Finish!\n", __FILE__, __func__);
    
    return 0;
}
