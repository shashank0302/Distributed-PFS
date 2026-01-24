#include "pfs_common.hpp"
#include<vector>
#include<ctime>
#include<string>
#include<sstream>
using namespace std;

// Get the current node's hostname
std::string getMyHostname() {
    char hostname[255] = {0};
    gethostname(hostname, sizeof(hostname));

    std::string ret(hostname);
    return ret;
};

// Get the current node's IP address
std::string getMyIP() {
    struct hostent *host_entry = gethostbyname(getMyHostname().c_str());
    char *temp = inet_ntoa(*((struct in_addr *)host_entry->h_addr_list[0]));

    std::string ret(temp);
    return ret;
}




string generateUniqueID(string filename, int clientID, int token_type, int startOffset, int endOffset) {
            time_t current_time = time(nullptr);
            ostringstream uniqueID;
            uniqueID << current_time << "-"
             << clientID << "-"
             << token_type << "-"
             << startOffset << "-"
             << endOffset<<"-"
             <<filename<<"-"
             <<rand();
            return uniqueID.str();
        }