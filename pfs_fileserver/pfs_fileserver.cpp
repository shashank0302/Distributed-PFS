#include "pfs_fileserver.hpp"
#include <filesystem>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using namespace std;
using namespace pfsfile;


class MyServiceImpl final : public myservice::Service {
    Status ConnectTest(ServerContext *context, const something* inputdata, ack* returndata ) override {
        cout<<"Server called Hell world"<<endl;
        returndata->set_val(1234);
        return Status::OK;
    }

    Status WriteFile(ServerContext* context, const FsWriteFileRequest* request, FsWriteFileResponse* response) override {
        string filename = request->filename();
        int server_no = request->server_no();
        int start_block_offset = request->start_block_offset();
        int end_block_offset = request->end_block_offset();
        string data = request->data();
        int pfs_block_size = request->pfs_block_size();

        string folder_path = string(getenv("HOME")) + "/team_curious/server_" + to_string(server_no);
        string file_path = folder_path + "/" + filename;

        try {
            if (!filesystem::exists(folder_path)) {
                filesystem::create_directories(folder_path);
            }

            fstream file(file_path, ios::in | ios::out | ios::binary);
            if (!file.is_open()) {
                file.open(file_path, ios::out | ios::binary); 
                file.close();
                file.open(file_path, ios::in | ios::out | ios::binary); 
            }

          
            file.seekg(0, ios::beg);
            vector<char> buffer(pfs_block_size, '0');
            vector<char> file_content(pfs_block_size, '0');
            file.read(file_content.data(), pfs_block_size);

            file.clear();

            for (int i = 0; i < pfs_block_size; ++i) {
                if (i < start_block_offset) {
                    buffer[i] = file_content[i];
                } else if (i > end_block_offset) {
                    buffer[i] = file_content[i];
                } else {
                    buffer[i] = data[i - start_block_offset];
                }
            }

            file.seekp(0, ios::beg); 
            file.write(buffer.data(), pfs_block_size);
            file.flush(); 

            file.close();
            string data_block(buffer.begin(), buffer.end());
            response->set_message("Write successful to " + file_path);
            response->set_success(true);
            response->set_blockdata(data_block);
        } catch (const exception& e) {
            response->set_success(false);
            response->set_message(string("Error: ") + e.what());
        }

        return Status::OK;
    }


    Status ReadFile(ServerContext* context, const FsReadFileRequest* request, FsReadFileResponse* response) override {
        
        string filename = request->filename();
        int server_no = request->server_no();
        size_t start_block_offset = request->start_block_offset();
        size_t end_block_offset = request->end_block_offset();
        int pfs_block_size = request->pfs_block_size();

        string folder_path = string(getenv("HOME")) + "/team_curious/server_" + to_string(server_no);
        string file_path = folder_path + "/" + filename;

        try {
            
            if (!filesystem::exists(folder_path)) {
                response->set_success(false);
                response->set_message("Folder does not exist: " + folder_path);
                return Status::OK;
            }

            
            if (!filesystem::exists(file_path)) {
                response->set_success(false);
                response->set_message("File does not exist: " + file_path);
                return Status::OK;
            }

            
            ifstream file(file_path, ios::in | ios::binary);
            if (!file.is_open()) {
                response->set_success(false);
                response->set_message("Failed to open file: " + file_path);
                return Status::OK;
            }

            
            vector<char> file_content(pfs_block_size, '0');
            file.read(file_content.data(), pfs_block_size);
            file.close();

            
            // if (start_block_offset < 0 || end_block_offset >= pfs_block_size || start_block_offset > end_block_offset) {
            //     response->set_success(false);
            //     response->set_message("Invalid block offsets");
            //     return Status::OK;
            // }

            
            int length = end_block_offset - start_block_offset + 1;
            string data(file_content.begin() + start_block_offset, file_content.begin() + start_block_offset + length);

            
            response->set_data(data);
            response->set_success(true);
            response->set_message("Read successful from " + file_path);

        } catch (const exception& e) {
            response->set_success(false);
            response->set_message(string("Error: ") + e.what());
        }

        return Status::OK;
    }


    Status DeleteFile(ServerContext* context, const FsDeleteFileRequest* request, FsDeleteFileResponse* response) {
        
        string filename = request->filename();
        int server_no = request->server_no();

        string folder_path = string(getenv("HOME")) + "/team_curious/server_" + to_string(server_no);
        string file_path = folder_path + "/" + filename;

        try {
        // Check if folder exists
            if (!std::filesystem::exists(folder_path)) {
                response->set_success(false);
                response->set_message("Folder does not exist: " + folder_path);
                return Status::OK;
            }

            // Check if file exists
            if (!std::filesystem::exists(file_path)) {
                response->set_success(false);
                response->set_message("File does not exist: " + file_path);
                return Status::OK;
            }

            if (std::remove(file_path.c_str()) == 0) {
                response->set_success(true);
                response->set_message("File deleted successfully: " + file_path);
            } else {
                response->set_success(false);
                response->set_message("Failed to delete file: " + file_path);
            }
        } catch (const std::exception& e) {
            response->set_success(false);
            response->set_message(std::string("Error: ") + e.what());
        }

        return Status::OK;
    }
};

void RunServer(string port) {
  string server_address = absl::StrFormat("%s:%s",getMyHostname(), port);
  MyServiceImpl myservice;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&myservice);
  // Finally assemble the server.
  unique_ptr<Server> server(builder.BuildAndStart());
  cout << "Server listening on " << server_address << endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char *argv[]) {
    printf("%s:%s: PFS file server start! Hostname: %s, IP: %s\n",
           __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    bool found = false;
    string line;
    getline(pfs_list, line); // First line is the meta server
    while (getline(pfs_list, line)) {
        if (line.substr(0, line.find(':')) == getMyHostname()) {
            found = true;
            break;
        }
    }
    if (!found) {
        fprintf(stderr, "%s: hostname not found in pfs_list.txt.\n", __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS fileserver and listen to requests
    printf("%s: Launching PFS file server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());
    
    RunServer(listen_port);

    // Do something...


    
    printf("%s:%s: PFS file server done!\n", __FILE__, __func__);
    return 0;
}
