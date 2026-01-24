#include "pfs_cache.hpp"
#include <optional>


using namespace std;

class Cache{
    private:
        size_t max_blocks;                           
        unordered_map<string, Node*> cache_map;      
        Node* head;                                  
        Node* tail;                                  
        mutex cache_mutex;
        struct pfs_execstat exec_stats;
        unordered_map<string, FileServerEntry> block_to_entry_map;      
    
    public:
        
        Cache(size_t max_blocks) : max_blocks(max_blocks),exec_stats{0, 0, 0, 0, 0, 0, 0} {}


        void initializecache(){
            std::lock_guard<std::mutex> lock(cache_mutex);
            while (head) {
                Node* temp = head;
                head = head->next;
                delete temp;
            }
            tail = nullptr;
            cache_map.clear();
        }

        optional<string> read_from_cache(const string& key){
            std::lock_guard<std::mutex> lock(cache_mutex);

            auto it = cache_map.find(key);
            if (it != cache_map.end()) {

                exec_stats.num_read_hits++;
                move_to_head(it->second);
                cout << "Came to cache" << endl;
                
                return it->second->block.data;
            }
            return nullopt;
        }

        void write_to_cache(const string& key, const string& data, bool is_dirty,int server) {
            std::lock_guard<std::mutex> lock(cache_mutex);

            time_t current_time = std::time(nullptr);
            
            auto it = cache_map.find(key);
            if (it != cache_map.end()) {

                exec_stats.num_write_hits++;
                Node* node = it->second;
                node->block.data = data;
                node->block.dirty = is_dirty;
                node->block.server_no=server;
                node->block.last_access_time=current_time;
                move_to_head(node);
            } else {
                if (cache_map.size() >= max_blocks) {
                    evict_LRU_block(); 

                }


                cacheBlock new_block{key, data, is_dirty, std::time(nullptr), server};
                Node* new_node = new Node(new_block);
                insert_at_head(new_node);
                cache_map[key] = new_node;
            }
        }

        bool find(const string& key){
            std::lock_guard<std::mutex> lock(cache_mutex);
            auto it = cache_map.find(key);
            if (it != cache_map.end()) {
                move_to_head(it->second);
                return true;
            }
            return false;

        }

        optional<cacheBlock> evict_block(const string& key,bool is_close_req){
            std::lock_guard<std::mutex> lock(cache_mutex);
            exec_stats.num_invalidations++;
            auto it = cache_map.find(key);
            cacheBlock temp;
            if (it != cache_map.end()) {
                Node* node = it->second;

                exec_stats.num_evictions++;
                if(is_close_req)
                    exec_stats.num_close_evictions++;
                if (node->block.dirty) {
                    cout<<"writing back the block"<<endl;
                    exec_stats.num_writebacks++;
                    if(is_close_req)
                        exec_stats.num_close_writebacks++;
                    temp=node->block;
                    
                    remove_node(node);
                    cache_map.erase(it);
                    delete node; 
                    return temp;
                }else{
                    cout<<"block is not dirty"<<endl;
                    remove_node(node);
                    cache_map.erase(it);
                    delete node; 
                    return nullopt;
                }

            }
            return nullopt;
        }

        void evict_LRU_block() {
            if (!tail) return;

            Node* lru_node = tail;
            exec_stats.num_evictions++;
            if (lru_node->block.dirty) {
                cout<<"block is dirty"<<endl;
                exec_stats.num_writebacks++;
                writeback(lru_node->block.blockname,lru_node->block.data,lru_node->block.server_no,PFS_BLOCK_SIZE);
            }

            remove_node(lru_node);
            cache_map.erase(lru_node->block.blockname);
            delete lru_node; 

        }

        int get_execution_stats(struct pfs_execstat *execstat_data){

            *execstat_data=exec_stats;
            return 0;

        }


    private:
        void move_to_head(Node* node) {
            if (node == head) return;

            remove_node(node);
            insert_at_head(node);
        }


        void insert_at_head(Node* node) {
            node->next = head;
            node->prev = nullptr;

            if (head) {
                head->prev = node;
            }
            head = node;

            if (!tail) {
                tail = head; 
            }
        }


        void remove_node(Node* node) {
            if (node->prev) {
                node->prev->next = node->next;
            } else {
                head = node->next; 
            }

            if (node->next) {
                node->next->prev = node->prev;
            } else {
                tail = node->prev; 
            }
        }



};

