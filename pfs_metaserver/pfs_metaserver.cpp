#include "pfs_metaserver.hpp"
#include<mutex>
#include<map>
#include<set>
#include<ctime>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReaderWriter;


using namespace pfsmeta;
using namespace std;

class FileRangeLock {
    private:
        unordered_map<string, pthread_mutex_t*> file_mutexes;
        pthread_mutex_t global_mutex;

    public:
        FileRangeLock() {
            pthread_mutex_init(&global_mutex, nullptr);
        }

        ~FileRangeLock() {
            pthread_mutex_destroy(&global_mutex);
            for (auto& pair : file_mutexes) {
                pthread_mutex_destroy(pair.second);
                delete pair.second;
            }
        }

        void lock_file(const string& filename) {
            pthread_mutex_t* mutex = get_or_create_mutex(filename);
            pthread_mutex_lock(mutex);
        }

        void unlock_file(const string& filename) {
            pthread_mutex_t* mutex = get_mutex(filename);
            if (mutex) {
                pthread_mutex_unlock(mutex);
            }
        }

        void lock_all() {
            pthread_mutex_lock(&global_mutex);
        }

        void unlock_all() {
            pthread_mutex_unlock(&global_mutex);
        }

        pthread_mutex_t* get_mutex_address(const string& filename) {
            return get_or_create_mutex(filename);
        }

    private:
    pthread_mutex_t* get_or_create_mutex(const string& filename) {
        pthread_mutex_lock(&global_mutex);
        pthread_mutex_t* mutex;
        auto it = file_mutexes.find(filename);
        if (it == file_mutexes.end()) {
            mutex = new pthread_mutex_t;
            pthread_mutex_init(mutex, nullptr);
            file_mutexes[filename] = mutex;
        } else {
            mutex = it->second;
        }
        pthread_mutex_unlock(&global_mutex);
        return mutex;
    }

    pthread_mutex_t* get_mutex(const string& filename) {
        pthread_mutex_lock(&global_mutex);
        pthread_mutex_t* mutex = nullptr;
        auto it = file_mutexes.find(filename);
        if (it != file_mutexes.end()) {
            mutex = it->second;
        }
        pthread_mutex_unlock(&global_mutex);
        return mutex;
    }
};

class MetaDataServiceImpl final : public metadataservice::Service {
    private:
        map<int,string> clients;
        unsigned int latest_client_id;
        mutex mutex_lock;
        map<string, PfsMetadata*> fileData;
        unsigned int file_descriptor_id;

        map<int, struct FileDescriptor> open_files;
        map<string, map<int,int>> open_files_by_clients;

        unordered_map<string, map<int, vector<Range>>> file_clients_ranges;

        FileRangeLock file_ranges_lock;

        mutex streams_mutex;
        unordered_map<int, ServerReaderWriter<TokenMessage, TokenMessage>*> client_streams;

        unordered_map<string, AcknowledgmentMapEntry> acknowledgment_map;
        pthread_mutex_t ack_map_mutex = PTHREAD_MUTEX_INITIALIZER;

        bool isRangeOverlapping(size_t start1, size_t end1, size_t start2, size_t end2) {
         return (start1 <= end2) && (start2 <= end1);
        }

        vector<pair<size_t, size_t>> findMissingRanges(string unique_file_identifier, size_t start_offset, size_t end_offset, int mode, int client_id) {
            vector<pair<size_t, size_t>> missing_ranges;


            if (file_clients_ranges.find(unique_file_identifier) == file_clients_ranges.end() || file_clients_ranges[unique_file_identifier].find(client_id) == file_clients_ranges[unique_file_identifier].end() ) {
                missing_ranges.push_back({start_offset, end_offset});
                return missing_ranges;
            }

            auto& ranges = file_clients_ranges[unique_file_identifier][client_id];

            sort(ranges.begin(), ranges.end(), [](const Range& a, const Range& b) {
                return a.start_offset < b.start_offset;
            });

            size_t current_start = start_offset;
            for (const auto& range : ranges) {
                
                if(range.mode < mode) {
                    continue;
                }

                if (range.start_offset > current_start) {
                    missing_ranges.push_back({current_start, min(range.start_offset - 1, end_offset)});
                }
                current_start = max(current_start, range.end_offset + 1);
                if (current_start > end_offset) break;
            }


            if (current_start <= end_offset) {
                missing_ranges.push_back({current_start, end_offset});
            }


            return missing_ranges;
        }
    
        vector<Range> adjustRanges(vector<Range>& ranges, size_t start_offset, size_t end_offset, int remove_mode) {
            vector<Range> updated_ranges, overlapping_ranges;
            cout<<"adjusting ranges for request"<<start_offset<<"- "<<end_offset<<endl;
            for (auto& range : ranges) {
                cout<<range.mode<<" range mode"<<endl;
                cout<<range.start_offset<<" "<<range.end_offset<<endl;
                cout<<isRangeOverlapping(range.start_offset, range.end_offset, start_offset, end_offset)<<endl;
                if ((range.mode != remove_mode) || !isRangeOverlapping(range.start_offset, range.end_offset, start_offset, end_offset)) {
                    cout<<"Inside non overlapped ranges"<<range.start_offset<<" "<<range.end_offset<<endl; 
                    updated_ranges.push_back(range);
                } else {
                    cout<<"Inside overlapped ranges"<<range.start_offset<<" "<<range.end_offset<<endl;
                    if (range.start_offset < start_offset) {
                        updated_ranges.push_back({range.start_offset, start_offset - 1, range.mode, range.client_id});
                    }
                    if (range.end_offset > end_offset) {
                        updated_ranges.push_back({end_offset + 1, range.end_offset, range.mode, range.client_id});
                    }
                    overlapping_ranges.push_back({
                        max(range.start_offset, start_offset),
                        min(range.end_offset, end_offset),
                        range.mode,
                        range.client_id
                    });
                }
            }

            ranges = updated_ranges;
            return overlapping_ranges;
        }
            
        void addRange(vector<Range>& ranges, size_t start_offset, size_t end_offset, int mode, int client_id) {
            ranges.push_back({start_offset, end_offset, mode, client_id});
        }
     

        vector<pair<size_t, size_t>> requestReadToken(const string& unique_file_identifier, int client_id, size_t start_offset, size_t end_offset) {
            file_ranges_lock.lock_file(unique_file_identifier);
            auto& client_ranges = file_clients_ranges[unique_file_identifier];

            vector<Range> revokingRanges;
            vector<pair<size_t, size_t>> missingRanges;

            cout << "Client " << client_id << " is requesting READ token for file '" << unique_file_identifier 
                << "' in range [" << start_offset << ", " << end_offset << "]\n";

            missingRanges = findMissingRanges(unique_file_identifier, start_offset, end_offset, 1, client_id);

            for (auto& [other_client_id, ranges] : client_ranges) {
                if(other_client_id != client_id) {
                    vector<Range> overlapping_ranges = adjustRanges(ranges, start_offset, end_offset, 2);
                    revokingRanges.insert(revokingRanges.end(), overlapping_ranges.begin(), overlapping_ranges.end());
                } 
            }

            if (!revokingRanges.empty()) {
                cout << "Revoking WRITE tokens for overlapping ranges:"<<endl;
                cout << "Waiting for revocation to complete..."<<endl;
                sendRevokeRequestsInParallel(unique_file_identifier, revokingRanges,false);
                
            } else {
                cout << "No overlapping WRITE tokens found. Proceeding...\n";
            }

            cout << "Granting READ token for Client " << client_id 
                << " in range [" << start_offset << ", " << end_offset << "]\n";

            addRange(client_ranges[client_id], start_offset, end_offset, 1, client_id);

            file_ranges_lock.unlock_file(unique_file_identifier);

            return missingRanges;
        }

        void sendRevokeRequestsInParallel(const string &unique_file_identifier, vector<Range> &revokingRanges,bool is_call_close) {
            vector<pthread_t> threads;
            cout << "Inside parallel requests" << endl;

            struct ThreadArgs {
                RevokeTokenRequest request;
                string uniqueId;
                MetaDataServiceImpl* instance; 
                int client_id;
            };

            for (const auto& range : revokingRanges) {
                cout << "  Overlapping range: [" << range.start_offset << ", " << range.end_offset
                    << "] Mode: "<< range.mode<< "(Client " << range.client_id << ")\n";

                RevokeTokenRequest request;
                request.set_filename(unique_file_identifier);
                request.set_start_offset(range.start_offset);
                request.set_end_offset(range.end_offset);
                request.set_token_type(range.mode);
                request.set_is_call_close(is_call_close);
                string uniqueId = generateUniqueID(unique_file_identifier, range.client_id, range.mode, range.start_offset, range.end_offset);
                request.set_unique_id(uniqueId);

                pthread_mutex_lock(&ack_map_mutex);
                pthread_cond_t* cv = new pthread_cond_t;
                pthread_cond_init(cv, nullptr);

                acknowledgment_map[uniqueId] = {false, cv, unique_file_identifier, range.client_id};
                pthread_mutex_unlock(&ack_map_mutex);

                ThreadArgs* args = new ThreadArgs{request, uniqueId, this, range.client_id};

                pthread_t thread;
                pthread_create(&thread, nullptr, [](void* arg) -> void* {
                    auto* threadArgs = static_cast<ThreadArgs*>(arg);
                    const auto& request = threadArgs->request;
                    const auto& uniqueId = threadArgs->uniqueId;
                    MetaDataServiceImpl* instance = threadArgs->instance;

                    instance->SendRevokeTokenRequest(threadArgs->client_id, request);

                    pthread_mutex_lock(&(instance->ack_map_mutex));
                    auto& ack_entry = instance->acknowledgment_map[uniqueId];
                    pthread_cond_t* cv = ack_entry.cv;

                    while (!ack_entry.is_completed) {
                        cout<<"waiting on conditional variable"<<endl;
                        pthread_cond_wait(cv, &(instance->ack_map_mutex));
                        cout<<"after wait unblocked"<<endl;
                    }

                    cout << "Acknowledgment received for UUID: " << uniqueId << "\n";

                    delete cv;
                    instance->acknowledgment_map.erase(uniqueId);
                    pthread_mutex_unlock(&(instance->ack_map_mutex));

                    delete threadArgs;
                    return nullptr;
                }, args);

                threads.push_back(thread);
            }

            for (auto& thread : threads) {
                pthread_join(thread, nullptr);
            }
        }


        
        vector<pair<size_t, size_t>> requestWriteToken(const string& unique_file_identifier, int client_id, size_t start_offset, size_t end_offset,TokenResponse* response) {
            
            file_ranges_lock.lock_file(unique_file_identifier);
            auto& client_ranges = file_clients_ranges[unique_file_identifier];

            vector<Range> revokingRanges;
            vector<pair<size_t, size_t>> missingRanges;

            cout << "Client " << client_id << " is requesting WRITE token for file '" << unique_file_identifier 
                << "' in range [" << start_offset << ", " << end_offset << "]\n";

            missingRanges = findMissingRanges(unique_file_identifier, start_offset, end_offset, 2, client_id);
            for (auto& [other_client_id, ranges] : client_ranges) {
                    
                    if(other_client_id != client_id) {

                        vector<Range> overlapping_read_ranges = adjustRanges(ranges, start_offset, end_offset, 1);

                        vector<Range> overlapping_write_ranges = adjustRanges(ranges, start_offset, end_offset, 2);

                        revokingRanges.insert(revokingRanges.end(), overlapping_read_ranges.begin(), overlapping_read_ranges.end());
                        
                        revokingRanges.insert(revokingRanges.end(), overlapping_write_ranges.begin(), overlapping_write_ranges.end()); 
                    }else{
                        vector<Range> self_read_ranges = adjustRanges(ranges ,start_offset ,end_offset , 1);
                        cout<<" inside self read ranges"<<endl;
                        for(auto range: self_read_ranges) {
                            cout<<range.start_offset<<" "<<range.end_offset<<" offsets in already present ranges"<<endl;
                            RangeMessage* rangeMessage = response->add_existingreadtokens();
                            rangeMessage->set_start_offset(range.start_offset);
                            rangeMessage->set_end_offset(range.end_offset);
                        }
                    }
            }

            if (!revokingRanges.empty()) {
                cout << "Revoking tokens for overlapping ranges:\n";
                for (const auto& range : revokingRanges) {
                    cout << "  Overlapping range: [" << range.start_offset << ", " << range.end_offset 
                        << "] Mode: " << (range.mode == 1 ? "Read" : "Write") 
                        << " (Client " << range.client_id << ")\n";
                }
                cout << "Waiting for revocation to complete...\n";
                sendRevokeRequestsInParallel(unique_file_identifier, revokingRanges,false);
            } else {
                cout << "No overlapping tokens found. Proceeding...\n";
                }
            cout << "Granting WRITE token for Client " << client_id 
                << " in range [" << start_offset << ", " << end_offset << "]\n";


            addRange(client_ranges[client_id], start_offset, end_offset, 2, client_id);

            file_ranges_lock.unlock_file(unique_file_identifier);
            return missingRanges;
        }
        
        void HandleTokenRequest(int client_id, const TokenRequest& request, ServerReaderWriter<TokenMessage, TokenMessage>* stream) {

            string unique_file_identifier = request.filename();
            size_t start_offset = request.start_offset();
            size_t end_offset = request.end_offset();
            int mode = request.token_type();
            string unique_id = request.unique_id();
            TokenMessage response_message;
            TokenResponse* response = response_message.mutable_token_response();
            cout<<"Received token request from client: "<<client_id;
            vector<pair<size_t, size_t>> newTokens;
            if(mode == 1) {
                newTokens = requestReadToken(unique_file_identifier, client_id, start_offset, end_offset);
            } else if (mode == 2 ) {
                newTokens = requestWriteToken(unique_file_identifier, client_id, start_offset, end_offset,response);
            }

            
            
            
            response->set_success(true);
            response_message.set_client_id(client_id);
            response->set_unique_id(unique_id);
            response->set_filename(unique_file_identifier);
            
            for(auto range: newTokens) {
                RangeMessage* rangeMessage = response->add_newtokens();
                rangeMessage->set_start_offset(range.first);
                rangeMessage->set_end_offset(range.second);
            }

            streams_mutex.lock();
            try {
                if(!stream->Write(response_message)) {
                    cout<<"Failed to write token response to client"<<endl;
                }
            }  catch (const std::exception &e) {
                    cout << "Exception while writing to the stream: for client "<<client_id << e.what() << endl;
            } catch (...) {
                    cout << "Unknown exception occurred while writing to the stream. for client"<<client_id << endl;
            }
            streams_mutex.unlock();
        }

        void SendRevokeTokenRequest(int client_id, const RevokeTokenRequest& request) {
            lock_guard<mutex> lock(streams_mutex);
            auto it = client_streams.find(client_id);
            if(it != client_streams.end()) {
                cout<<"inside streams"<<endl;
                TokenMessage message;
                *message.mutable_revoke_token_request() = request;
                try {
                    if(it->second && !it->second->Write(message)) {
                        cout<<"Unable to send token revoke message to the client"<<client_id<<endl;
                    } else {
                        cout<<"written to the stream"<<endl;
                        }
                }  
                catch (const std::exception &e) {
                    cout << "Exception while writing to the stream: for client "<<client_id << e.what() << endl;
                } 
                catch (...) {
                    cout << "Unknown exception occurred while writing to the stream. for client"<<client_id << endl;
                }
            } else {
                cout<<"Client "<<client_id<<"not connected."<<endl;
            }
        }
        
        void HandleRevokeTokenResponse (int client_id, const RevokeTokenResponse& response) {
            cout<<"Received Token Response from the client"<<client_id<<endl;
            string unique_id = response.unique_id();

            pthread_mutex_lock(&ack_map_mutex);
            pthread_cond_t* cv = acknowledgment_map[unique_id].cv;
            acknowledgment_map[unique_id].is_completed = true;
            pthread_cond_signal(cv);
            pthread_mutex_unlock(&ack_map_mutex);
        }

        void removeAllTokens(int client_id) {
            cout<<"removing all tokens allocated to"<<client_id<<endl;
            file_ranges_lock.lock_all();

            for (auto it = file_clients_ranges.begin(); it != file_clients_ranges.end(); ) {
                auto& client_map = it->second;

                auto inner_it = client_map.find(client_id);
                if (inner_it != client_map.end()) {
                    client_map.erase(inner_it);
                } 
                it++;
            
            }
            file_ranges_lock.unlock_all();

        }

        void revocateAllTokensByFilenameAndClient(string unique_file_identifier, int client_id,bool is_call_close) {
            try {
                cout << "Removing all tokens for filename: " << unique_file_identifier << endl;

                file_ranges_lock.lock_file(unique_file_identifier);

                
                if (file_clients_ranges.find(unique_file_identifier) != file_clients_ranges.end()) {
                    auto& client_map = file_clients_ranges[unique_file_identifier];
                    
                    if (client_map.find(client_id) != client_map.end()) {
                        vector<Range> revokingRanges = client_map[client_id];
                        sendRevokeRequestsInParallel(unique_file_identifier, revokingRanges,is_call_close);
                        client_map.erase(client_id);
                    } else {
                        cout << "No ranges found for client_id: " << client_id << " under filename: " << unique_file_identifier << endl;
                    }
                } else {
                    cout << "No token ranges found for filename: " << unique_file_identifier << endl;
                }
            }
            catch(exception& e) {
                cout<<e.what()<<"inside exception"<<endl;
            }
            file_ranges_lock.unlock_file(unique_file_identifier);
        }

        void revocateAllTokensByClient(int client_id) {
            cout << "Removing all tokens for client_id: " << client_id << endl;
            file_ranges_lock.lock_all();
            
            for (auto& file_entry : file_clients_ranges) {
                auto& client_map = file_entry.second;
                if (client_map.find(client_id) != client_map.end()) {
                   vector<Range> revokingRanges = client_map[client_id];
                   sendRevokeRequestsInParallel(file_entry.first, revokingRanges,false);
                   client_map.erase(client_id);
                }
            }

            file_ranges_lock.unlock_all();

        }

        void autoAckWaitingOnClient(int client_id) {
            cout<<"auto acknowledging all pending requests from "<<client_id<<endl;
            pthread_mutex_lock(&ack_map_mutex);

            for (auto& [key, entry] : acknowledgment_map) {
                if (entry.client_id == client_id) {
                    entry.is_completed = true;
                    pthread_cond_signal(entry.cv);
                }
            }
            pthread_mutex_unlock(&ack_map_mutex);
        }

        bool disconnect_client(int client_id) {
            mutex_lock.lock();
            {
                lock_guard<mutex> lock(streams_mutex);
                client_streams.erase(client_id);
            }
            auto it = open_files_by_clients.begin();
            while (it != open_files_by_clients.end()) {
                it->second.erase(client_id);
                ++it;
            }
            auto it1 = open_files.begin();
            while (it1 != open_files.end()) {
                if (it1->second.client_id == client_id) {
                    it1 = open_files.erase(it1);
                    cout<<"erasing client from open_files"<<endl;

                }else{
                    ++it1;
                }
            }
            auto it2 = clients.find(client_id);
            if (it2 != clients.end()) {
                clients.erase(it2);
            }

           
            mutex_lock.unlock();
            removeAllTokens(client_id);
            autoAckWaitingOnClient(client_id);

            return true;
        }

        vector<FileServerBlock> calculate_stripe_blocks(string filename,
                             off_t offset,
                             size_t num_bytes, 
                             int stripe_width, 
                             int pfs_block_size, 
                             int stripe_blocks
                        ) {
            size_t bytes_per_server = pfs_block_size * stripe_blocks;
            size_t starting_server = (offset / bytes_per_server) % stripe_width;

            size_t server_offset = offset % bytes_per_server;

            size_t completed_bytes = 0;
            int completed_blocks_in_server = server_offset / pfs_block_size;
            size_t starting_bytes_of_block = offset - server_offset + (completed_blocks_in_server * pfs_block_size);


            vector<FileServerBlock> fileblocks_in_server;

            while (completed_bytes < num_bytes) {
                size_t ending_bytes_of_block = starting_bytes_of_block + pfs_block_size - 1;

                ostringstream oss;
                oss << filename << "_server_" << starting_server << "_" << starting_bytes_of_block << "_" << ending_bytes_of_block;
                string filename_in_server = oss.str();

                FileServerBlock block;
                block.set_block_name(filename_in_server);
                block.set_server(starting_server);
                block.set_starting_bytes_of_block(starting_bytes_of_block);
                block.set_ending_bytes_of_block(ending_bytes_of_block);

                fileblocks_in_server.push_back(block);

                completed_blocks_in_server += 1;
                starting_bytes_of_block += pfs_block_size;
                completed_bytes += min(pfs_block_size - server_offset, num_bytes - completed_bytes);
                server_offset = 0;
                if (completed_blocks_in_server >= stripe_blocks) {
                    completed_blocks_in_server = 0;
                    starting_server = (starting_server + 1) % stripe_width;
                }
            }


            return fileblocks_in_server;
        }




    public:
        MetaDataServiceImpl(): latest_client_id(0), file_descriptor_id(0){};

        Status initializeClient(ServerContext *context, const initialData* inputData, initialAck* returnData) override {
            string clienthostname = inputData->hostname();
            string clientport = inputData->port();
            cout<<"Got the client request with hostname: "<<clienthostname<<"with port: "<<clientport<<endl;
            int clientid;
            mutex_lock.lock();
                latest_client_id++;
                clients[latest_client_id] = clienthostname+clientport;
                clientid=latest_client_id;
            mutex_lock.unlock();
            returnData->set_client_id(clientid);
            cout<<"I decided clientid as "<<clientid<<endl;
            return Status::OK;
        }

        Status createFile(ServerContext *context, const CreateFileData* reqData, PfsMetadata* pfsMetaData) override {
            string filename = reqData->filename();
            int stripe_width = reqData->stripe_width();
            int num_of_fileservers = reqData->num_servers();

            if(stripe_width>num_of_fileservers) {
                return Status::CANCELLED;
            }
            pfsMetaData->set_filename(filename);
            PfsFileRecipe* recipe = pfsMetaData->mutable_recipe();
            recipe->set_stripe_width(stripe_width);
            recipe->set_stripe_blocks(reqData->stripe_blocks());
            recipe->set_pfs_block_size(reqData->pfs_block_size());

            mutex_lock.lock();
                if(fileData.find(filename) !=fileData.end() ){
                    mutex_lock.unlock();
                    return Status::CANCELLED;
                } else {
                    PfsMetadata* metadata = new PfsMetadata();
                    metadata->CopyFrom(*pfsMetaData);

                    time_t current_time = time(nullptr);
                    metadata->set_ctime(current_time);
                    metadata->set_mtime(0);
                    metadata->set_unique_id(rand());
                    metadata->set_unique_file_identifier( filename 
                                                + to_string(metadata->unique_id())
                                                + to_string(metadata->ctime())
                                                );
                    cout<<"file created successfully"<<endl;
                    cout<<"current files in system:"<<endl;
                    fileData[filename] = metadata;

                    printData();
                    mutex_lock.unlock();
                   return Status::OK;
                }
            
        }

        Status openFile(ServerContext *context, const PfsOpenFile* reqData, PfsFileDescriptorId * response) override {
            string filename = reqData->filename();
            int mode = reqData->mode();
            int clientid = reqData->client_id();
            
            FileDescriptor filedescriptor;
            mutex_lock.lock();
            if(fileData.find(filename) ==fileData.end()) {
                mutex_lock.unlock();
                cout<<"file not present"<<endl;
                return Status::CANCELLED;
            } else {
                file_descriptor_id++;
                filedescriptor.fd_id = file_descriptor_id;
                filedescriptor.access_mode = mode;
                filedescriptor.client_id=clientid;
                filedescriptor.filename = filename;
                filedescriptor.marked_for_deletion = false;
                
                filedescriptor.metadata = fileData[filename];
                
                filedescriptor.unique_file_identifier = filename 
                                                + to_string(filedescriptor.metadata->unique_id())
                                                + to_string(filedescriptor.metadata->ctime());
        
                if(open_files_by_clients.find(filedescriptor.unique_file_identifier) == open_files_by_clients.end()) {
                    open_files_by_clients[filedescriptor.unique_file_identifier][clientid] = filedescriptor.fd_id;
                } else {
                    if(open_files_by_clients[filedescriptor.unique_file_identifier].find(clientid) != open_files_by_clients[filedescriptor.unique_file_identifier].end()) {
                        cout<<"duplicate opening of file"<<endl;
                        mutex_lock.unlock();
                        return Status::CANCELLED;
                    }
                    open_files_by_clients[filedescriptor.unique_file_identifier][clientid]=filedescriptor.fd_id;

                }
                // print_open_files_by_clients();
                PfsMetadata *metadata = response->mutable_metadata();
                metadata->CopyFrom(*filedescriptor.metadata);
                response->set_fd(file_descriptor_id);
                open_files[file_descriptor_id] = filedescriptor;
                mutex_lock.unlock();
                cout<<"file :"<<filename<<" opened successfully in client id: "<<clientid<<endl;

                return Status::OK;
            }
        }

        Status writeFile(ServerContext *context, const PfsWriteFile* reqData, PfsMetadata* response) override {
            unsigned int fdid = reqData->fd();
            size_t num_bytes = reqData->num_bytes();
            off_t offset = reqData->offset();

            mutex_lock.lock();
            if(open_files.find(fdid)==open_files.end()) {
                cout<<"Invalid file descritpor"<<endl;
                mutex_lock.unlock();
                return Status::CANCELLED;
            } else {
                FileDescriptor fd = open_files[fdid];
                int mode = fd.access_mode;
                if(mode != 2) {
                    cout<<"Invalid access mode"<<endl;
                    mutex_lock.unlock();
                    return Status::OK;
                }                
                

                long file_size = fd.metadata->file_size();
                
                long new_file_size = offset+num_bytes;

                if(offset > file_size || offset < 0) {
                    cout<<"wrong offset"<<endl;
                    mutex_lock.unlock();
                    return Status::CANCELLED;
                }
                
                if(new_file_size > file_size) {
                    fd.metadata->set_file_size(new_file_size);
                    vector<FileServerBlock> file_blocks = calculate_stripe_blocks(
                                                            fd.unique_file_identifier,
                                                            offset,
                                                            num_bytes,
                                                            fd.metadata->recipe().stripe_width(),
                                                            fd.metadata->recipe().pfs_block_size(),
                                                            fd.metadata->recipe().stripe_blocks()
                                                );
                    
                    for(auto fileBlock: file_blocks) {
                        FileServerBlock* file_server_block = fd.metadata->add_block_addresses();
                        file_server_block->CopyFrom(fileBlock);
                    }
        
                }
                mutex_lock.unlock();

                response->CopyFrom(*fd.metadata);

                printData();
                return Status::OK;

            }
            

        }

        Status readFile(ServerContext *context, const PfsReadFile* reqData, PfsMetadata* response) override {
            int fdid = reqData->fd();
            long offset = reqData->offset();

            mutex_lock.lock();
            if(open_files.find(fdid)==open_files.end()) {
                cout<<"Invalid file descritpor"<<endl;
                mutex_lock.unlock();
                return Status::CANCELLED;
            } else {
                FileDescriptor fd = open_files[fdid];
                mutex_lock.unlock();

                response->CopyFrom(*fd.metadata);

                long file_size = response -> file_size();

                if(offset > file_size || offset < 0) {
                    cout<<"wrong offset"<<endl;
                    return Status::CANCELLED;
                }
                return Status::OK;
            }
            
        }


        void printData() {
            for(auto x: fileData) {
                    cout<<x.first<<" :"<<endl;
                    cout<<"filename: "<<x.second->filename()<<"file_size: "<<x.second->file_size()<<"creation time: "<<x.second->ctime()<<"modified time: "<<x.second->mtime()
                        <<"recipe:\t stripe_width: "<<x.second->recipe().stripe_width()<<"stripe_blocks: "<<x.second->recipe().stripe_blocks()<<"pfs_block_size: "<<x.second->recipe().pfs_block_size()<<endl;
                }

        }

        Status CloseFile(ServerContext* context, const CloseFileRequest* request, CloseFileResponse* response){
            try {
                // printFileClientsRanges();
                int fdid = request->fd();
                string unique_file_identifier;
                mutex_lock.lock();
                auto it = open_files.find(fdid);
                FileDescriptor file_descriptor;
                
                int client_id;
                if (it != open_files.end()) {
                    client_id = open_files[fdid].client_id;
                    unique_file_identifier = open_files[fdid].unique_file_identifier;
                    file_descriptor = open_files[fdid];
                    open_files.erase(fdid);
                    response->set_success(true);
                    cout<<"File descriptor " << fdid << " closed successfully." << endl;
                    
                }else{
                    mutex_lock.unlock();
                    cerr << "Invalid file descriptor: " << fdid << endl;
                    response->set_success(false);
                    return Status::CANCELLED;
                }
                
                
                time_t current_time = time(nullptr);
                file_descriptor.metadata->set_mtime(current_time);


                auto it1=open_files_by_clients.find(file_descriptor.unique_file_identifier);
                if(it1 != open_files_by_clients.end()){
                    open_files_by_clients[file_descriptor.unique_file_identifier].erase(client_id);
                } else{
                    cerr << "Invalid file name: " << unique_file_identifier << endl;
                }
                // print_open_files_by_clients();
                response->set_should_delete_file(false);
                if(file_descriptor.marked_for_deletion && open_files_by_clients[file_descriptor.unique_file_identifier].empty()){
                    response->set_should_delete_file(true);


                }

                PfsMetadata* metadata=response->mutable_metadata();
                metadata->CopyFrom(*file_descriptor.metadata);
                if(response->should_delete_file()){
                    delete file_descriptor.metadata;
                }
                revocateAllTokensByFilenameAndClient(unique_file_identifier, request->client_id(),true);
                cout<<"I am handling"<<endl;
                // printFileClientsRanges();
                mutex_lock.unlock();
            } catch (exception& e) {
                cout<<"received exception"<<e.what()<<endl;
            }

            return Status::OK;
        }

        Status DisconnectClient(ServerContext* context, const DisconnectClientRequest* request, DisconnectClientResponse* response){
            // printFileClientsRanges();
            int client_id=request->client_id();
            if(disconnect_client(client_id)) {
                response->set_success(true);
                return Status::OK;
            } else {
                response->set_success(false);
                return Status::CANCELLED;
            }
            
            revocateAllTokensByClient(client_id);
            // printFileClientsRanges();

        }

        Status Fstat(ServerContext* context, const PfsFstatRequest* request, PfsMetadata* response) {
            int fdid = request->fd();
            mutex_lock.lock();
            auto it = open_files.find(fdid);
            
            if (it != open_files.end()) {
                response->CopyFrom(*open_files[fdid].metadata);
                mutex_lock.unlock();
                return Status::OK;
            } else {
                mutex_lock.unlock();
                return Status::CANCELLED;
            }
        }

        Status DeleteFile(ServerContext* context, const DeleteFileRequest* request, DeleteFileResponse* response) {
            // printFileClientsRanges();
            string filename = request->filename();
            mutex_lock.lock();
            auto it= fileData.find(filename);
            string unique_file_identifier;
            if(it != fileData.end()){
                PfsMetadata* metadata = response->mutable_metadata();
                metadata->CopyFrom(*fileData[filename]);
                unique_file_identifier = metadata->unique_file_identifier();
        
                fileData.erase(it);
                cout<<"File " << filename << " deleted successfully." << endl;
                
            }else{
                cerr << "File " << filename << " not found." << endl;
                mutex_lock.unlock();
                return Status::CANCELLED;
            }
            auto open_files_it=open_files_by_clients.find(unique_file_identifier);
            // print_open_files_by_clients();
            if(open_files_it != open_files_by_clients.end() && !open_files_by_clients[unique_file_identifier].empty()){
                for(auto open_clients: open_files_by_clients[unique_file_identifier]) {
                    open_files[open_clients.second].marked_for_deletion = true;
                }
                cout<<"file marked for deletion "<<unique_file_identifier<<endl;
                mutex_lock.unlock();
                response->set_is_delete_delayed(true);
                return Status::OK;
            } 
            response->set_is_delete_delayed(false);
            mutex_lock.unlock();
            

            // printFileClientsRanges();
            return Status::OK;
        }

        Status TokenStream(ServerContext* context, ServerReaderWriter<TokenMessage, TokenMessage>* stream ) override {
            
            const auto& metadata = context->client_metadata();
            auto it = metadata.find("client-id");
            int client_id = -1;

            if(it != metadata.end()) {
                client_id = stoi(string(it->second.data(), it->second.size()));
                cout<<"Extracted client_id from metadata:"<<client_id<<endl;
                lock_guard<mutex> lock(streams_mutex);
                client_streams[client_id] = stream;
                
            }

            TokenMessage client_message;

            while(stream->Read(&client_message)) {
                if(client_message.has_token_request()) {
                     thread handleTokenRequest(
                        &MetaDataServiceImpl::HandleTokenRequest,
                         this, 
                         client_id, 
                         client_message.token_request(), 
                         ref(stream)
                        );

                     handleTokenRequest.detach();
                } else if (client_message.has_revoke_token_response()) {
                    cout<<"received a revoke token response from client"<<endl;
                    thread handleRevokeTokenResponse(
                        &MetaDataServiceImpl::HandleRevokeTokenResponse,
                        this,
                        client_id,
                        client_message.revoke_token_response()
                        );
                    handleRevokeTokenResponse.detach();
                    
                }
            }
            cout<<"Stream ended on server side"<<client_id<<endl;
            
            disconnect_client(client_id);
            


            return Status::OK;
        }
        
};

void RunServer(string port) {
  string server_address = absl::StrFormat("%s:%s",getMyHostname(), port);
  MetaDataServiceImpl myservice;

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
    printf("%s:%s: PFS meta server start! Hostname: %s, IP: %s\n", __FILE__,
           __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    string line;
    getline(pfs_list, line);
    if (line.substr(0, line.find(':')) != getMyHostname()) {
        fprintf(stderr, "%s: hostname not on the first line of pfs_list.txt.\n",
                __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS metadata server and listen to requests
    printf("%s: Launching PFS metadata server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...
    RunServer(listen_port);


    
    printf("%s:%s: PFS meta server done!\n", __FILE__, __func__);
    return 0;
}
