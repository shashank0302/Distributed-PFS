#include "pfs_api.hpp"
#include "pfs_cache.cpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"
#include <grpcpp/grpcpp.h>
#include "pfs_proto/pfs_fileserver.pb.h"
#include "pfs_proto/pfs_fileserver.grpc.pb.h"
#include "pfs_proto/pfs_metaserver.pb.h"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
#include "pfs_common/pfs_config.hpp"
#include <optional>
#include <vector>
#include <algorithm>
#include <mutex>

using namespace pfsmeta;
using namespace pfsfile;
using namespace std;
using namespace grpc;


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


string metadataserver;
vector<string> file_servers;
int assigned_clientid;

mutex result_mutex;
mutex read_result_mutex;
mutex delete_result_mutex;
mutex stream_write_mutex;
Cache client_cache(CLIENT_CACHE_BLOCKS);


map<string, vector<TokenRangeClient>> token_ranges;

FileRangeLock token_ranges_lock;

unordered_map<string, AcknowledgmentMapEntry> acknowledgment_map;
pthread_mutex_t ack_map_mutex = PTHREAD_MUTEX_INITIALIZER;

map<string, set<BusyTokenRange*>> busy_ranges;
pthread_mutex_t busy_ranges_mutex = PTHREAD_MUTEX_INITIALIZER;

unordered_map<string,int> filename_fd;
unordered_map<int,PfsMetadata> fd_metadata;
unordered_map<int,int> fd_accessmode;

mutex cache_maps_mutex;




BusyTokenRange* markRangeBusy(const string& unique_file_identifier, size_t start_offset, size_t end_offset,  int mode) {
    
    pthread_mutex_lock(&busy_ranges_mutex); 
    
        auto& ranges = busy_ranges[unique_file_identifier];
        BusyTokenRange *range = new BusyTokenRange({start_offset, end_offset, true, PTHREAD_COND_INITIALIZER, mode});
        ranges.insert(range);
     
    pthread_mutex_unlock(&busy_ranges_mutex);
    
    return range;
}

void checkOverlapAndWait(const string& filename, size_t start_offset, size_t end_offset, int mode) {
    pthread_mutex_lock(&busy_ranges_mutex);
     
    cout<<"acquired busy ranges lock"<<"\n";
    auto& ranges = busy_ranges[filename];

    auto it = ranges.begin();
    vector<BusyTokenRange*> overlapping_busy_ranges;
    while (it != ranges.end()) {
        auto range = *it;
        cout<<"range"<<range->is_busy<<" "<<range->start_offset <<" "<<range->end_offset<<" "<<range->mode<<"\n";
        if (range->is_busy && range->mode == mode&& (start_offset < range->end_offset && end_offset > range->start_offset)) {
            overlapping_busy_ranges.push_back(range);
            cout<<"overlapping range"<<"\n";
            }
            it++;
            }
    if(!overlapping_busy_ranges.empty()) {
        cout<<"overlapping"<<"\n";
        for(auto range: overlapping_busy_ranges) {
            while(range->is_busy) {
                cout<<"range is  busy waiting"<<"\n";
                pthread_cond_wait(&range->cv, &busy_ranges_mutex);
            }
            cout<<"range is not busy acquired range"<<"\n";
        }
    } else {
        cout<<"not overlapping range"<<"\n";
    }
     
    pthread_mutex_unlock(&busy_ranges_mutex);
}

void markRangeNotBusy(BusyTokenRange *range) {    
    range->is_busy = false;
    pthread_cond_signal(&range->cv);
     
}


class FileServerClient {
    public:
        FileServerClient(shared_ptr<Channel> channel) 
            : channel_(channel) {}
    
    int ConnectTest() {
        something request;
        ack reply;
        request.set_aaa(1234);
        request.set_bbb(12345);
        

        ClientContext context;

         auto stub_ = myservice::NewStub(channel_);
        Status status = stub_->ConnectTest(&context, request,&reply );


        if(status.ok()) {
            cout<< "received" + to_string(reply.val())<<"\n";
            return 0;
        } else {
            cout << status.error_code() << ": " << status.error_message()
                << "\n";
            return -1;
        }

    }
    int writeBack(const string& filename,
                    int starting_server,
                    int start_block_offset,
                    int end_block_offset,
                    const string& data,
                    int pfs_block_size) {

        string key = filename;
        cout<<"inside write back function call"<<"\n";

        FsWriteFileRequest request;
        FsWriteFileResponse response;

        request.set_filename(filename);
        request.set_server_no(starting_server);
        request.set_start_block_offset(start_block_offset);
        request.set_end_block_offset(end_block_offset);
        request.set_data(data);
        request.set_pfs_block_size(pfs_block_size);

        ClientContext context;

         auto stub_ = myservice::NewStub(channel_);
        Status status = stub_->WriteFile(&context, request, &response);
        if (status.ok()) {
            cout << "Success: " << response.message() << " from server " << starting_server << "\n";
            string data = response.blockdata();

        } else {
            cerr << "Error: RPC failed for server " << starting_server << "\n";
            return -1;
        }
        return 0;  
    
    }

    int writeFile(const string& filename,
                    int starting_server,
                    int start_block_offset,
                    int end_block_offset,
                    const string& data,
                    int pfs_block_size) {
        
        string key = filename;
        cout<<"entered write block"<<"\n";
        auto cached_data = client_cache.read_from_cache(key);
        string block_data(pfs_block_size, '\0'); 

        if(cached_data.has_value()){
            cout<<"block found in cache"<<"\n";
            memcpy(block_data.data(), cached_data->data(), pfs_block_size);

            memcpy(block_data.data() + start_block_offset, data.data(), end_block_offset - start_block_offset + 1);
           
            client_cache.write_to_cache(key, block_data,true,starting_server);
            return 0;
        }
        cout<<"block not found in cache"<<"\n";

        FsWriteFileRequest request;
        FsWriteFileResponse response;

        request.set_filename(filename);
        request.set_server_no(starting_server);
        request.set_start_block_offset(start_block_offset);
        request.set_end_block_offset(end_block_offset);
        request.set_data(data);
        request.set_pfs_block_size(pfs_block_size);

        ClientContext context;

         auto stub_ = myservice::NewStub(channel_);
        Status status = stub_->WriteFile(&context, request, &response);
        if (status.ok()) {
            cout << "Success: " << response.message() << " from server " << starting_server << "\n";
            string data = response.blockdata();

            block_data=data;

        } else {
            cerr << "Error: RPC failed for server " << starting_server << "\n";
            return -1;
        }
        
        client_cache.write_to_cache(key, block_data,true,starting_server); 
        
        return 0;        
        

    }

    int readFile(const string& filename,
                    int server,
                    int start_block_offset,
                    int end_block_offset,
                    int relative_start_position_from_offset,
                    int relative_end_position_from_offset,
                    size_t starting_bytes_of_block,
                    size_t ending_bytes_of_block,
                    void* buf,
                    int pfs_block_size
                ) {
        
        string key = filename;
        cout<<"read block"<<"\n";
        auto cached_data = client_cache.read_from_cache(key);
        if(cached_data.has_value()) {
            // Cache hit: Copy data from cache to buffer
             cout<<"read block found in cache"<<"\n";
            memcpy(static_cast<char*>(buf) + relative_start_position_from_offset ,cached_data->data() + start_block_offset, end_block_offset - start_block_offset + 1);
            return 0;         
        }

        cout<<"read block not found in cache"<<"\n";
        FsReadFileRequest request;
        FsReadFileResponse response;

        request.set_filename(filename);
        request.set_server_no(server);
        request.set_start_block_offset(0);
        request.set_end_block_offset(pfs_block_size-1);
        request.set_pfs_block_size(pfs_block_size);

        ClientContext context;
         auto stub_ = myservice::NewStub(channel_);
        Status status = stub_->ReadFile(&context, request, &response);

        if(status.ok()) {
            cout<< "Success: "<<response.message()<<"from server"<< server<<"\n";
            
            string data = response.data();


            int length = end_block_offset - start_block_offset + 1;

            client_cache.write_to_cache(key,data,false,server);
            memcpy((char*)buf + relative_start_position_from_offset, data.data()+ start_block_offset, length);

            return 0;
        } else {
            cerr << "Error: RPC failed for server " << server << "\n";
            return -1;
        }
                    
    }

    int deleteFile(const string& filename,int server) {
        FsDeleteFileRequest request;
        FsDeleteFileResponse response;

        request.set_filename(filename);
        request.set_server_no(server);

        ClientContext context;
         auto stub_ = myservice::NewStub(channel_);
        Status status = stub_->DeleteFile(&context, request, &response);

        if(status.ok()) {
            cout<< "Success: "<<response.message()<<"from server"<< server<<"\n";
            return 0;
        } else {
            cerr << "Error: RPC failed for server " << server << "\n";
            return -1;
        }
                    
    }
    
    private:
    shared_ptr<Channel> channel_;
};

class MetaDataServerClient {
    private:
        shared_ptr<Channel> channel_;
        unique_ptr<ClientContext> stream_context_;
        unique_ptr<ClientReaderWriter<TokenMessage, TokenMessage>> stream_;

        
        void ListenForServerMessages() {
            TokenMessage server_message;
            cout<<"I am listening for any messages from the server"<<"\n";
            while(stream_->Read(&server_message)) {
                cout<<" received some message from server"<<"\n";
                cout<<"is it token response"<<server_message.has_token_response();
                cout<<"is it revoke token request"<<server_message.has_revoke_token_request()<<"\n";
                if(server_message.has_token_response()) {
                    thread([&]() {
                        TokenResponse tokenResponse = server_message.token_response();
                        string uniqueId = tokenResponse.unique_id();

                        for (const auto& rangeMessage : tokenResponse.newtokens()) {
                            cout << "Range: " << rangeMessage.start_offset() << " - " << rangeMessage.end_offset() << "\n";
                            invalidateCache(rangeMessage.start_offset(), rangeMessage.end_offset(), tokenResponse.filename(),false);
                        }

                        for(const auto& rangeMessage : tokenResponse.existingreadtokens()){
                            cout << "Range: " << rangeMessage.start_offset() << " - " << rangeMessage.end_offset() <<" is alreaduy exisitng but in read mode" <<"\n";
                            token_ranges_lock.lock_file(tokenResponse.filename());
                            relinquishTokens(tokenResponse.filename(),rangeMessage.start_offset(), rangeMessage.end_offset(), 1);
                            token_ranges_lock.unlock_file(tokenResponse.filename());
                        }
                        
                        pthread_mutex_lock(&ack_map_mutex);
                        if(acknowledgment_map.count(uniqueId) != 0) {
                            pthread_cond_t* cv = acknowledgment_map[uniqueId].cv;
                            acknowledgment_map[uniqueId].is_completed = true;
                            pthread_cond_signal(cv);
                        }
                        pthread_mutex_unlock(&ack_map_mutex);
                    }).detach();

                } else if (server_message.has_revoke_token_request()) {

                    thread handleTokenRevocation(
                        &MetaDataServerClient::HandleTokenRevocation,
                        this,
                        server_message.revoke_token_request()
                    );
                    handleTokenRevocation.detach();
                }
            }

            cout<<"Stream closed"<<"\n";
        }

        bool isRangeOverlapping(size_t start1, size_t end1, size_t start2, size_t end2) {
         return (start1 <= end2) && (start2 <= end1);
        }

        void relinquishTokens(string filename, size_t start_offset, size_t end_offset, int token_type) {
            cout<<"inside relinquish Tokens"<<"\n";
            token_ranges_lock.lock_all();
            if (token_ranges.find(filename) != token_ranges.end()) {
                auto& ranges = token_ranges[filename];
                token_ranges_lock.unlock_all();
                vector<TokenRangeClient> updated_ranges;
                
                for (auto& range : ranges) {
                    if (range.mode != token_type || !isRangeOverlapping(range.start_offset, range.end_offset, start_offset, end_offset)) {
                        updated_ranges.push_back(range);
                    } else {
                        if (range.start_offset < start_offset) {
                            pthread_cond_t* new_cond = new pthread_cond_t;
                            pthread_cond_init(new_cond, NULL);
                            updated_ranges.push_back({range.start_offset, start_offset - 1, range.mode});
                        }
                        if (range.end_offset > end_offset) {
                            pthread_cond_t* new_cond = new pthread_cond_t;
                            pthread_cond_init(new_cond, NULL);
                            updated_ranges.push_back({end_offset + 1, range.end_offset, range.mode});
                        }
                        
                    }
                }
                
                cout<<"before upadte ranges"<<"\n";
                ranges = updated_ranges;
                cout<<"after updated ranges"<<"\n";
            } else {
                token_ranges_lock.unlock_all();
            }
        }
        void HandleTokenRevocation(const RevokeTokenRequest& request) {
            TokenMessage ack_message;
            RevokeTokenResponse* response = ack_message.mutable_revoke_token_response();
            response->set_success(true);
            response->set_unique_id(request.unique_id());
            response->set_is_call_close(request.is_call_close());
            cout<<"received token revocation request from server for file:"<<request.filename()
                <<"\t end_offset:" << request.end_offset()
                <<"\t start_offset:"<< request.start_offset()
                <<"\t token type:"<< request.token_type()
                <<"\t is it closing request: "<<request.is_call_close()<<"\n";
            
            token_ranges_lock.lock_file(request.filename());
            cout<<"acquired lock"<<"\n";
            checkOverlapAndWait(request.filename(), request.start_offset(), request.end_offset(), request.token_type());
            relinquishTokens(request.filename(), request.start_offset(), request.end_offset(), request.token_type());
            cout<<"token relinquished"<<"\n";
            
            invalidateCache(request.start_offset(), request.end_offset(), request.filename(),response->is_call_close());


            

            cout<<"unlocked the mutex"<<"\n";
            token_ranges_lock.unlock_file(request.filename());
            stream_write_mutex.lock();
            if(!stream_->Write(ack_message)) {
                cout<<"Failed to write revoke token response to stream."<<"\n";
            } else {
                cout<<"successfully written to the server"<<"\n";
            }
            stream_write_mutex.unlock();
          
        }

    public:
        MetaDataServerClient(shared_ptr<Channel> channel) : channel_(channel) {}

        int initializeClient() {
            initialData request;
            initialAck response;

            request.set_hostname(getMyHostname());
            request.set_port("1234");

            ClientContext context;
             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->initializeClient(&context,request, &response );

            if(status.ok()) {
                cout<<"client id = "+ to_string(response.client_id());
                return response.client_id();
            } else {
                cout<< "received error";
                return -1;
            }
        
        }

        int createFile(const char* filename, int stripe_width) {
            CreateFileData reqData;
            PfsMetadata pfsMetaData;
            
            reqData.set_filename(string(filename));
            reqData.set_stripe_width(stripe_width);
            reqData.set_stripe_blocks(STRIPE_BLOCKS);
            reqData.set_pfs_block_size(PFS_BLOCK_SIZE);
            reqData.set_num_servers(NUM_FILE_SERVERS);

            ClientContext context;

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->createFile(&context, reqData, &pfsMetaData);

            if(status.ok()) {
                cout<<"File Created"<<"\n";
                return 0;
            } else {
                cout<<"File Creation Failed"<<"\n";
                return -1;
            }


        }

        int openFile(const char* filename, int mode, int assigned_clientid) {
            PfsOpenFile reqData;
            PfsFileDescriptorId responseData;

            reqData.set_filename(string(filename));
            reqData.set_mode(mode);
            reqData.set_client_id(assigned_clientid);

            ClientContext context;

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->openFile(&context, reqData, &responseData);

            if(status.ok()) {
                cout<<"File Opened and file descriptor returned"<< responseData.fd();
                cache_maps_mutex.lock();
                filename_fd[responseData.metadata().unique_file_identifier()]=responseData.fd();
                fd_metadata[responseData.fd()] = responseData.metadata();
                fd_accessmode[responseData.fd()]=mode;
                cache_maps_mutex.unlock();
                return responseData.fd();
            } else {
                return -1;
            }

        }

        optional<PfsMetadata> writeFile(int fd, size_t num_bytes, off_t offset) {

            cache_maps_mutex.lock();
            auto it=fd_metadata.find(fd);
            PfsMetadata meta_data;
            if(fd_accessmode[fd] !=2 ) {
                cout<<"returning since incorrect file mode"<<"\n";
                 cache_maps_mutex.unlock();
                return nullopt;
            }
            if(it != fd_metadata.end()){
                size_t actual_file_size = it->second.file_size();
                size_t req_file_size = num_bytes + offset;
                cout << "Actual file size: " << actual_file_size << ", Requested file size: " << req_file_size << "\n";
                if(actual_file_size >= req_file_size){
                    cache_maps_mutex.unlock();
                    return it->second;
                }
            }
            cache_maps_mutex.unlock();

            PfsWriteFile reqData;
            PfsMetadata response;

            reqData.set_fd(fd);
            reqData.set_num_bytes(num_bytes);
            reqData.set_offset(offset);

            ClientContext context;

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->writeFile(&context, reqData, &response);

            if(status.ok()) {
                meta_data = response;
                cache_maps_mutex.lock();
                fd_metadata[fd]=meta_data;
                cache_maps_mutex.unlock();
                return meta_data;
            } else {
                cout<<"received status not ok from metadata server while writing file"<<"\n";
                return nullopt;
            }

        }


        optional<PfsMetadata> readFile(int fd, size_t num_bytes, off_t offset) {
            cache_maps_mutex.lock();
            auto it=fd_metadata.find(fd);
            PfsMetadata meta_data;
            if(it != fd_metadata.end()){
                size_t actual_file_size = it->second.file_size();
                size_t req_file_size = num_bytes + offset;
                cout << "Actual file size: " << actual_file_size << ", Requested file size: " << req_file_size << "\n";
                if(actual_file_size >= req_file_size){
                    cache_maps_mutex.unlock();
                    return it->second;
                }
            }
            cache_maps_mutex.unlock();
            PfsReadFile reqData;
            PfsMetadata response;

            reqData.set_fd(fd);
            reqData.set_num_bytes(num_bytes);
            reqData.set_offset(offset);

            ClientContext context;

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->readFile(&context, reqData, &response);

            if(status.ok()) {
                meta_data = response;
                cache_maps_mutex.lock();
                fd_metadata[fd]=meta_data;
                cache_maps_mutex.unlock();
                return meta_data;
            } else {
                cout<<"received status not ok from metadataserver while read file"<<"\n";
                return nullopt;
            }
        }

        optional<CloseFileResponse> closeFile(int fd){
            ClientContext context;
            CloseFileRequest request;
            CloseFileResponse response;
            request.set_fd(fd);
            request.set_client_id(assigned_clientid);

             auto stub_ = metadataservice::NewStub(channel_);
            Status status=stub_->CloseFile(&context,request,&response);

            if(status.ok() && response.success()){
                cout<<"file closed successfully"<<"\n";
                return response;
            }else{
                cout<<"received status not ok from metadataserver while closing file"<<"\n";
                return nullopt;                
            }
        }

        int disconnectClient(int client_id){
            ClientContext context;
            DisconnectClientRequest request;
            DisconnectClientResponse response;
            request.set_client_id(client_id);

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->DisconnectClient(&context, request, &response);

            if (status.ok() && response.success()) {
                cout << "Disconnected successfully from metadata server." << "\n";
                return true;
            } else {
                cerr << "Failed to disconnect from metadata server: " << status.error_message() << "\n";
                return false;
            }            
        }

        optional<DeleteFileResponse> deletefile(const char *filename){
            ClientContext context;
            DeleteFileRequest request;
            DeleteFileResponse response;

            request.set_filename(filename);
            request.set_client_id(assigned_clientid);

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->DeleteFile(&context, request, &response);

            if (status.ok()) {
                // PfsMetadata meta_data = response;
                cout << "File deleted successfully" << "\n";
                return response;
            } else {
                // cerr << "Failed to delete file: " << status.error_message() << "\n";
                cout<<"received status not ok from metadataserver while deleting file"<<"\n";
                return nullopt;
            }

        }

        int fstat(int fd,pfs_metadata *meta_data){
            PfsFstatRequest request;
            request.set_fd(fd);

            PfsMetadata response;
            ClientContext context;

             auto stub_ = metadataservice::NewStub(channel_);
            Status status = stub_->Fstat(&context, request, &response);

            if (status.ok()) {
                // Populate the meta_data structure with the response
                strncpy(meta_data->filename, response.filename().c_str(), sizeof(meta_data->filename));
                meta_data->file_size = response.file_size();
                meta_data->ctime = response.ctime();
                meta_data->mtime = response.mtime();
                meta_data->recipe.stripe_width = response.recipe().stripe_width();
                meta_data->recipe.stripe_blocks = response.recipe().stripe_blocks();
                meta_data->recipe.pfs_block_size = response.recipe().pfs_block_size();
                meta_data->unique_id = response.unique_id();
                meta_data->unique_file_identifier = response.unique_file_identifier();

                return 0;
            }else{
                // cerr << "Failed to retrieve metadata: " << status.error_message() << "\n";
                return -1;
            }
        }

        void StartTokenStream() {
            stream_context_ = make_unique<ClientContext>(); 
            stream_context_->AddMetadata("client-id", to_string(assigned_clientid));

             auto stub_ = metadataservice::NewStub(channel_);
            stream_ = stub_->TokenStream(stream_context_.get());
            if(stream_)
                cout<<"Grpc biderctional stream created"<<"\n";
            else 
                cout<<"Error creating grpc biderctional stream"<<"\n";

            thread reader_thread(&MetaDataServerClient::ListenForServerMessages, this);
            reader_thread.detach();
        }

        bool RequestToken(const string& unique_file_identifier, int64_t start_offset, int64_t end_offset, const int& token_type, int client_id) {
            TokenMessage message;
            message.set_client_id(client_id);

            TokenRequest* request = message.mutable_token_request();
            request->set_filename(unique_file_identifier);
            request->set_start_offset(start_offset);
            request->set_end_offset(end_offset);
            request->set_token_type(token_type);

            string uniqueId = generateUniqueID(unique_file_identifier, client_id,token_type, start_offset, end_offset);

            uniqueId = uniqueId + "client";
            
            cout<<"requesting token from "<<start_offset<<" to "<<end_offset<<"filename: "<<unique_file_identifier<<"\n";
            
            pthread_mutex_lock(&ack_map_mutex);
            pthread_cond_t* cv = new pthread_cond_t;
            pthread_cond_init(cv, nullptr);

            acknowledgment_map[uniqueId] = {false, cv, unique_file_identifier, client_id};
            pthread_mutex_unlock(&ack_map_mutex);


            request->set_unique_id(uniqueId);
            

            cout<<"Request prepared"<<"\n";
            try {
                if(!stream_) {
                    StartTokenStream();
                    cout<<"earlier stream is not found, starting a new one"<<"\n";
                }

                stream_write_mutex.lock();
                if (stream_&&!stream_->Write(message)) {
                    cout << "Failed to write token request to stream." << "\n";
                    return false;
                }
                stream_write_mutex.unlock();

               

            } catch(exception &e) {
                cout<<e.what()<<"\n";
            }

            auto& ack_entry = acknowledgment_map[uniqueId];
            while(!ack_entry.is_completed) {
                cout<<"waiting for the acknowledgment and giving back mutex"<<"\n";
                pthread_cond_wait(cv, token_ranges_lock.get_mutex_address(unique_file_identifier));
            }
            cout<<"Acknowledgment received for unique_id"<<uniqueId<<"\n";
            addTokenRange(unique_file_identifier, start_offset, end_offset, token_type, assigned_clientid);
            return true;
        }

        
        void addTokenRange(string unique_file_identifier, size_t start_offset, size_t end_offset,int mode, int client_id ) {
            if (token_ranges.find(unique_file_identifier) != token_ranges.end()) {
                auto& ranges = token_ranges[unique_file_identifier];
                
                vector<TokenRangeClient> updated_ranges;
                
                for (auto& range : ranges) {
                    if (range.mode != mode || !isRangeOverlapping(range.start_offset, range.end_offset, start_offset, end_offset)) {
                        updated_ranges.push_back(range);
                    } else {
                        
                        if (range.start_offset < start_offset) {
                            pthread_cond_t* new_cond = new pthread_cond_t;
                            pthread_cond_init(new_cond, NULL);
                            updated_ranges.push_back({range.start_offset, start_offset - 1, range.mode});
                        }
                        if (range.end_offset > end_offset) {
                            pthread_cond_t* new_cond = new pthread_cond_t;
                            pthread_cond_init(new_cond, NULL);
                            updated_ranges.push_back({end_offset + 1, range.end_offset, range.mode});
                        }
                    }
                }
                
          
                ranges = updated_ranges;
                pthread_cond_t* cv = new pthread_cond_t;
                pthread_cond_init(cv, nullptr);

                TokenRangeClient new_token_range;
                new_token_range.start_offset = start_offset;
                new_token_range.end_offset = end_offset;
                new_token_range.mode = mode;
                
                
                ranges.push_back(new_token_range);
            }
        
            
            cout << "Added token range: [" << start_offset << ", " << end_offset << "] for mode: " << mode << " and client_id: " << client_id << "\n";

        }

        vector<pair<size_t, size_t>> findMissingRanges(string unique_file_identifier, size_t start_offset, size_t end_offset, int mode) {
            vector<pair<size_t, size_t>> missing_ranges;


            if (token_ranges.find(unique_file_identifier) == token_ranges.end()) {
                missing_ranges.push_back({start_offset, end_offset});
                return missing_ranges;
            }

            auto& ranges = token_ranges[unique_file_identifier];

            sort(ranges.begin(), ranges.end(), [](const TokenRangeClient& a, const TokenRangeClient& b) {
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

        vector<TokenRangeClient*> findOverlappingRanges(string filename, size_t start_offset, size_t end_offset, int mode) {
            vector<TokenRangeClient*> overlapping_ranges;


            if (token_ranges.find(filename) == token_ranges.end()) {
                return overlapping_ranges;
            }

            auto& ranges = token_ranges[filename];

            for (auto& range : ranges) {
                if(range.mode < mode) {
                    continue;
                }

                if (range.end_offset >= start_offset && range.start_offset <= end_offset) {
                    overlapping_ranges.push_back(&range);
                }
            }


            return overlapping_ranges;
        }
};


int calculate_block_offset(off_t offset, long bytes_per_server, int pfs_block_size) {
    long server_offset = offset % bytes_per_server;
    int block_offset = server_offset % pfs_block_size;
    return block_offset;
}

vector<FileServerEntry> calculate_stripe_blocks(string filename, off_t offset, size_t num_bytes, int stripe_width, int pfs_block_size, int stripe_blocks)  
{
    size_t bytes_per_server = pfs_block_size * stripe_blocks;
    size_t starting_server = (offset / bytes_per_server) % stripe_width;

    size_t server_offset = offset % bytes_per_server;

    size_t completed_bytes = 0;
    int completed_blocks_in_server = server_offset / pfs_block_size;
    size_t starting_bytes_of_block = offset - server_offset + (completed_blocks_in_server * pfs_block_size);
    size_t final_bytes = offset + num_bytes - 1;

    vector<FileServerEntry> filenames_in_server;

    while (completed_bytes < num_bytes) {
        size_t ending_bytes_of_block = starting_bytes_of_block + pfs_block_size - 1;

        ostringstream oss;
        oss << filename << "_server_" << starting_server << "_" << starting_bytes_of_block << "_" << ending_bytes_of_block;
        string filename_in_server = oss.str();

        int start_block_offset = calculate_block_offset(
            max(static_cast<size_t>(starting_bytes_of_block), static_cast<size_t>(offset)),
            bytes_per_server, 
            pfs_block_size
        );

        int end_block_offset = calculate_block_offset(
            min(static_cast<size_t>(ending_bytes_of_block), final_bytes),
            bytes_per_server, 
            pfs_block_size
        );

        filenames_in_server.push_back(
            FileServerEntry{
                filename_in_server,       
                static_cast<int>(starting_server), 
                starting_bytes_of_block,          
                ending_bytes_of_block,            
                start_block_offset,       
                end_block_offset,
                starting_bytes_of_block - offset + start_block_offset,
                starting_bytes_of_block - offset + end_block_offset        
            }
        );

        completed_blocks_in_server += 1;
        starting_bytes_of_block += pfs_block_size;
        completed_bytes += min(pfs_block_size - server_offset, num_bytes - completed_bytes);
        server_offset = 0;
        if (completed_blocks_in_server >= stripe_blocks) {
            completed_blocks_in_server = 0;
            starting_server = (starting_server + 1) % stripe_width;
        }
    }

    // Print the results
    // for (const auto& entry : filenames_in_server) {
    //     cout << "Filename: " << entry.filename
    //          << ", Server: " << entry.server_no
    //          << ", Start Bytes: " << entry.starting_bytes_of_block
    //          << ", End Bytes: " << entry.ending_bytes_of_block
    //          << ", Start Block Offset: " << entry.start_block_offset
    //          << ", End Block Offset: " << entry.end_block_offset
    //          <<", relative start position from offset"<< entry.relative_start_position_from_offset
    //          <<", relative end position from offset"<< entry.relative_end_position_from_offset
    //         << "\n";
    // }

    return filenames_in_server;
}


MetaDataServerClient *metadataserverclient = NULL;
unordered_map<string, FileServerClient*> fileserverclients;
mutex file_server_clients_mutex;

void writeback(string blockname, string data, int server, int pfs_block_size){
    file_server_clients_mutex.lock();
    FileServerClient* fileserverclient = fileserverclients[file_servers[server]];
    file_server_clients_mutex.unlock();
    int result = fileserverclient->writeBack(blockname,server,0, pfs_block_size, data, pfs_block_size);
    

    if(result == 0){ 
        cout<<"write back completed for the block successfully"<<blockname<<"\n";
        return;
    }else{
        cout<<"write back failed for the block"<<blockname<<"\n";
    }

}


void invalidateCache(size_t start_offset,size_t end_offset,string unique_file_identifier,bool is_close_req){

    cache_maps_mutex.lock();
    int fd=filename_fd[unique_file_identifier];
    auto it = fd_metadata.find(fd);
   

    PfsMetadata metadata;
    if(it != fd_metadata.end()){
        metadata=it->second;
        cout<<"this is filesize coming to invalidate"<<metadata.file_size()<<"\n";
    }
     cache_maps_mutex.unlock();

    int stripe_width = metadata.recipe().stripe_width();
    int pfs_block_size = metadata.recipe().pfs_block_size();
    int stripe_blocks = metadata.recipe().stripe_blocks();
    // size_t filesize=metadata.file_size();

    vector<FileServerEntry> filenames_in_server = calculate_stripe_blocks(unique_file_identifier, start_offset, end_offset-start_offset+1, stripe_width,pfs_block_size, stripe_blocks);
    for(auto &entry: filenames_in_server){
        if(client_cache.find(entry.filename)){
            cout<<"going to cache to evict a block"<<"\n";

            auto block_optional = client_cache.evict_block(entry.filename,is_close_req);
            cacheBlock block_to_write;
            if(block_optional){
                block_to_write = *block_optional;
                
                writeback(block_to_write.blockname,block_to_write.data,entry.server_no,pfs_block_size);
            }
        }
    }

}


int pfs_initialize() {
    // Read pfs_list.txt
    ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    
    getline(pfs_list, metadataserver); // First line is the meta server
    string file_server;
    while (getline(pfs_list, file_server)) {
        file_servers.push_back(file_server);
    }

    // Check if all servers (NUM_FILE_SERVERS + 1) are online
    // Connect with metaserver using gRPC
    metadataserverclient = new MetaDataServerClient(CreateChannel(metadataserver, grpc::InsecureChannelCredentials()));

    int client_id = metadataserverclient->initializeClient();
    assigned_clientid = client_id;
    metadataserverclient->StartTokenStream();
    cout<<"token stream started"<<"\n";

    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    for (int i = 0; i < NUM_FILE_SERVERS; ++i) {
        cout<<"connecting to file server"<<file_servers[i]<<endl;
        FileServerClient* client = new FileServerClient(CreateChannel(file_servers[i], grpc::InsecureChannelCredentials()));
        fileserverclients[file_servers[i]] = client;
        int status = client->ConnectTest();
        if(status != 0) {
            return -1;
        } 
    }

    //initialize the exec_stat structure
    client_cache.initializecache();
    




    // int client_id = 1;
    cout<<"Received client id= "<< client_id<<" "<<"\n";

    return client_id;
}

int pfs_finish(int client_id) {
    //free(exec_stats);
    try{
        if (!metadataserverclient->disconnectClient(client_id)) {
                cerr << "Failed to disconnect from metadata server." << "\n";
                return -1;
        }

        cout << "PFS client finished successfully." << "\n";
        return 0; 
    }catch (const exception& e) {
        cerr << "Exception in pfs_finish: " << e.what() << "\n";
        return -1;
    }
}

int pfs_create(const char *filename, int stripe_width) {
    return metadataserverclient->createFile(filename, stripe_width);
}

int pfs_open(const char *filename, int mode) {


    return metadataserverclient->openFile(filename, mode, assigned_clientid);
}


void grpcReadCall(
    const string server_address,
    const FileServerEntry &entry,
    void* buf,
    int pfs_block_size,
    vector<int>& results,
    size_t result_index
    ) 
{

    file_server_clients_mutex.lock();
    FileServerClient* fileserverclient = fileserverclients[server_address];
    file_server_clients_mutex.unlock();
    int result = fileserverclient->readFile(entry.filename,
                            entry.server_no,entry.start_block_offset, 
                            entry.end_block_offset, 
                            entry.relative_start_position_from_offset, 
                            entry.relative_end_position_from_offset, 
                            entry.starting_bytes_of_block,
                            entry.ending_bytes_of_block,
                            buf, 
                            pfs_block_size
                        );
    
    lock_guard<mutex> lock(read_result_mutex);
    if(result == 0) {
        results[result_index] = entry.end_block_offset - entry.start_block_offset +1;
    } else {
        results[result_index] = -1;
    }
    
}

void makeParallelGrpcReadCalls(const vector<FileServerEntry>& filenames_in_server,const vector<string>& file_servers,void* buf,size_t num_bytes,int pfs_block_size,vector<int>& results)
{
    results.resize(filenames_in_server.size(), -1);
    vector<thread> threads;

    for (size_t i = 0; i < filenames_in_server.size(); ++i) {
        const FileServerEntry& entry = filenames_in_server[i];
        string server_address = file_servers[entry.server_no ];

        threads.emplace_back(
            grpcReadCall,
            move(server_address),
            entry,
            buf,
            pfs_block_size,
            ref(results),
            i
        );
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
     try {
    // Check client cache
        auto metadataserver_result = metadataserverclient->readFile(fd, num_bytes, offset);
        PfsMetadata metadata_extracted ;

        if(metadataserver_result) {
            metadata_extracted = *metadataserver_result;
        } else {
            cout<<"Failed to retrieve metadata"<<"\n";
            return -1;
        }

        int stripe_width = metadata_extracted.recipe().stripe_width();
        int pfs_block_size = metadata_extracted.recipe().pfs_block_size();
        int stripe_blocks = metadata_extracted.recipe().stripe_blocks();
        string unique_file_identifier = metadata_extracted.unique_file_identifier();

        token_ranges_lock.lock_file(unique_file_identifier);

        vector<pair<size_t, size_t>> missingRanges = metadataserverclient->findMissingRanges(unique_file_identifier, offset, offset+num_bytes, 1);


        if (!missingRanges.empty()) {
            cout<<"Ranges are missing requesting token to metadataserver";
            metadataserverclient->RequestToken(unique_file_identifier, offset, offset + num_bytes - 1 , 1, assigned_clientid);

        } else {
            metadataserverclient->addTokenRange(unique_file_identifier, offset, offset + num_bytes -1, 1, assigned_clientid);
        }
        
        BusyTokenRange *range1 = markRangeBusy(unique_file_identifier, offset, offset+num_bytes - 1, 1);
        BusyTokenRange *range2 = markRangeBusy(unique_file_identifier, offset, offset+num_bytes -1 , 2);
        size_t actual_num_bytes = metadata_extracted.file_size() - offset;
     
        num_bytes = min(actual_num_bytes, num_bytes);
    
        vector<FileServerEntry> filenames_in_server = calculate_stripe_blocks(unique_file_identifier, offset, num_bytes, stripe_width,pfs_block_size, stripe_blocks);
        vector<int> results;
        makeParallelGrpcReadCalls(filenames_in_server, file_servers, buf, num_bytes, pfs_block_size, results);
        markRangeNotBusy(range1);
        markRangeNotBusy(range2);
        token_ranges_lock.unlock_file(unique_file_identifier);
        
        unsigned long long ans = 0ll;
        for(int x: results) {
            if (x == -1)    
                return -1;
                
            ans +=x;
        }

        if (!metadataserver_result) {
            cerr << "Failed to update metadata on the server" << "\n";          
            return -1;
        }
        return ans;

    } catch (const exception& e) {
        
        cerr << "Exception: " << e.what() << "\n";
    }

    // ...

    return 0;
}

void grpcWriteCall(
    const string server_address,
    const FileServerEntry &fileEntry,
    const string& data,
    int pfs_block_size,
    vector<int>& results,
    size_t result_index
    ) 
{
    file_server_clients_mutex.lock();
    FileServerClient* fileserverclient = fileserverclients[server_address];
    file_server_clients_mutex.unlock();
    int result = fileserverclient->writeFile(fileEntry.filename,fileEntry.server_no,fileEntry.start_block_offset, fileEntry.end_block_offset, data, pfs_block_size);
    
    lock_guard<mutex> lock(result_mutex);
    if(result == 0) {
        results[result_index] = fileEntry.end_block_offset - fileEntry.start_block_offset +1;
    } else {
        results[result_index] = -1;
    }
    
}

void makeParallelGrpcWriteCalls(
    const vector<FileServerEntry>& filenames_in_server,
    const vector<string>& file_servers,
    const void* buf,
    size_t num_bytes,
    int pfs_block_size,
    vector<int>& results)
{
    results.resize(filenames_in_server.size(), -1);
    vector<thread> threads;

    for (size_t i = 0; i < filenames_in_server.size(); ++i) {
        const FileServerEntry& entry = filenames_in_server[i];
        string server_address = file_servers[entry.server_no ];
        size_t start_offset = entry.relative_start_position_from_offset;
        size_t end_offset = entry.relative_end_position_from_offset;

      

        const char* data_slice = static_cast<const char*>(buf) + start_offset;
        size_t data_length = end_offset - start_offset + 1;

        threads.emplace_back(
            grpcWriteCall,
            move(server_address),
            entry,
            string(data_slice, data_length),
            pfs_block_size,
            ref(results),
            i
        );
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}


int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    // ...
    try {
    // Check client cache
        auto metadataserver_result = metadataserverclient->writeFile(fd, num_bytes, offset);
        PfsMetadata metadata_extracted ;

        if(metadataserver_result) {
            metadata_extracted = *metadataserver_result;
            cout<<"Hello world inside"<<"\n";
        } else {
            cout<<"Failed to retrieve metadata"<<"\n";
            return -1;
        }

        int stripe_width = metadata_extracted.recipe().stripe_width();
        int pfs_block_size = metadata_extracted.recipe().pfs_block_size();
        int stripe_blocks = metadata_extracted.recipe().stripe_blocks();
        string unique_file_identifier = metadata_extracted.unique_file_identifier();

        token_ranges_lock.lock_file(unique_file_identifier);

        vector<pair<size_t, size_t>> missingRanges = metadataserverclient->findMissingRanges(unique_file_identifier, offset, offset+num_bytes, 2);

        if (!missingRanges.empty()) {
            cout<<"Ranges are missing requesting token to metadataserver";
            metadataserverclient->RequestToken(unique_file_identifier, offset, offset+num_bytes - 1, 2, assigned_clientid);
            
        } else {
            metadataserverclient->addTokenRange(unique_file_identifier, offset, offset+num_bytes - 1 , 2, assigned_clientid);
        }

        BusyTokenRange* range1 = markRangeBusy(unique_file_identifier, offset, offset+num_bytes - 1, 2);

       
    
        vector<FileServerEntry> filenames_in_server = calculate_stripe_blocks(unique_file_identifier, offset, num_bytes, stripe_width,pfs_block_size, stripe_blocks);
        vector<int> results;
        makeParallelGrpcWriteCalls(filenames_in_server, file_servers, buf, num_bytes, pfs_block_size, results);
        
        markRangeNotBusy(range1);
        
        token_ranges_lock.unlock_file(unique_file_identifier);

        unsigned long long ans = 0ll;
        for(int x: results) {
            if (x == -1)    
                return -1;
            ans +=x;
        }

        return ans;

    } catch (const exception& e) {
        
        cerr << "Exception: " << e.what() << "\n";
    }




    // ...

    return 0;
}

void grpcDeleteCall(const string server_address,const FileServerBlock &entry,vector<int>& results,size_t result_index) {

    file_server_clients_mutex.lock();
    FileServerClient* fileserverclient = fileserverclients[server_address];
    file_server_clients_mutex.unlock();
    int result = fileserverclient->deleteFile(entry.block_name(),entry.server());

    lock_guard<mutex> lock(delete_result_mutex);
    if(result == 0) {
        results[result_index] = 1;
    } else {
        results[result_index] = -1;
    }
    
}

void makeParallelGrpcDeleteCalls(google::protobuf::RepeatedPtrField<FileServerBlock>& filenames_in_server,const vector<string>& file_servers,vector<int>& results){
    results.resize(filenames_in_server.size(), -1);
    vector<thread> threads;

    for (int i = 0; i < filenames_in_server.size(); ++i) {
        const FileServerBlock& entry = filenames_in_server[i];
        string server_address = file_servers[ entry.server() ];
    

        threads.emplace_back(
            grpcDeleteCall,
            move(server_address),
            entry,
            ref(results),
            i
        );
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}



int pfs_close(int fd) {

    if (fd < 0) {
        cerr << "Invalid file descriptor" << "\n";
        return -1;
    }
    try {

        auto metadataserver_result = metadataserverclient->closeFile(fd);
        CloseFileResponse close_response_extracted;
        if(metadataserver_result) {
            close_response_extracted = *metadataserver_result;
        } else {
            cout<<"Failed to retrieve metadata"<<"\n";
            return -1;
        }     
        PfsMetadata metadata_extracted= close_response_extracted.metadata();

        auto filenames_in_server = metadata_extracted.block_addresses();
        vector<int> results;
        if(close_response_extracted.should_delete_file()) {
            makeParallelGrpcDeleteCalls(filenames_in_server, file_servers,results);
        }

        return 0;

    }catch(const exception& e){
        cerr << "Exception: " << e.what() << "\n";
    }
    return 0;
}


int pfs_delete(const char *filename) 
{

    try {

        auto metadataserver_result = metadataserverclient->deletefile(filename);
        DeleteFileResponse delete_response_extracted;
        if(metadataserver_result) {
            delete_response_extracted = *metadataserver_result;
        } else {
            cout<<"Failed to retrieve metadata"<<"\n";
            return -1;
        }

        PfsMetadata metadata_extracted= delete_response_extracted.metadata();

    
        auto filenames_in_server = metadata_extracted.block_addresses();
        vector<int> results;
        if(!delete_response_extracted.is_delete_delayed()) {
            makeParallelGrpcDeleteCalls(filenames_in_server, file_servers,results);
        }
        unsigned long long ans = 0ll;
        for(int x: results) {
            if (x == -1)    
                return -1;
            ans +=x;
        }

        return 0;

    } catch (const exception& e) {
        
        cerr << "Exception: " << e.what() << "\n";
    }
    return 0;
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data) {
    
    return metadataserverclient->fstat(fd,meta_data);
}

int pfs_execstat(struct pfs_execstat *execstat_data) {
    
    return client_cache.get_execution_stats(execstat_data);
    
}