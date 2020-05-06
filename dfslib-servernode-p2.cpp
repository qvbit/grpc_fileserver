#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>
#include <google/protobuf/util/time_util.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using namespace dfs_service;
using namespace std;

using google::protobuf::util::TimeUtil;
using google::protobuf::Timestamp;

using FileRequestType = FileName;
using FileListResponseType = AllFiles;

extern dfs_log_level_e DFS_LOG_LEVEL;

class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;


    /* SYNCHRONIZATION CONSTRUCTS
    / ------------------------------------------- */

    // Use a shared_timed_mutex to solve the problem of releasing lock -> timeout if held too long.
    // Map stores information about which client has a lock on which file.
    shared_timed_mutex file_to_cid_mutex;
    // Filename to client_id map
    map<string, string> file_to_cid_map;
    // This is the actual rw mutex
    shared_timed_mutex rw_mutex;
    // Filename to mutex map
    map<string, unique_ptr<shared_timed_mutex>> rw_map;
    // Mutex for directory
    shared_timed_mutex async_mutex;

    void ReleaseClientMutex(string file_name) {
        file_to_cid_mutex.lock();
        file_to_cid_map.erase(file_name);
        file_to_cid_mutex.unlock();
    }

    void AddFileMutex(string file_name) {
        rw_mutex.lock();
        // If filename not already in map, add it as the key and value a new unique mutex.
        if (rw_map.find(file_name) == rw_map.end()) {
            rw_map[file_name] = make_unique<shared_timed_mutex>();
        }
        rw_mutex.unlock();
        
    }

    shared_timed_mutex* GetFileMutex(string file_name) {
        // Lock shared so mutiple clients can get the mutex at the same time. 
        rw_mutex.lock_shared();
        auto file_mutex_pair = rw_map.find(file_name);
        shared_timed_mutex* file_mutex = file_mutex_pair->second.get();
        rw_mutex.unlock_shared();
        return file_mutex;
    }

    void UnlockAll(string filename) {
        ReleaseClientMutex(filename);
        file_to_cid_mutex.unlock();
        async_mutex.unlock();
    }

    /* ------------------------------------------- */

    // Method to match checksum
    Status ChecksumsDiffer(uint32_t client_crc, string& filepath) {
        // if (client_crc == 0) {
        //     dfs_log(LL_SYSINFO) << "Client sent empty CRC";
        //     return Status(StatusCode::CANCELLED, "No client crc");
        // }
        uint32_t server_crc = dfs_file_checksum(filepath, &crc_table);
        dfs_log(LL_SYSINFO) << "Comparing crcs... Filepath: " << filepath << ", Client crc: " << client_crc << ", Server crc: " << server_crc;

        if (client_crc == server_crc) {
            dfs_log(LL_SYSINFO) << "Both file contents on server and client are identical";
            return Status(StatusCode::ALREADY_EXISTS, "ALREADY_EXISTS");
        }
        return Status::OK;
    }

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

        /* Code to add existing files at mount and associated mutex to mapping table
        / ----------------------------------------------------------------------------- */
        DIR *dir = opendir(mount_path.c_str());
        if (dir == NULL) {
            dfs_log(LL_ERROR) << "Error opening directory at mount path";
            exit(1);
        }

        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            struct stat fstat;
            string dir_entry = ent->d_name;
            string filepath = this->WrapPath(dir_entry);
            stat(filepath.c_str(), &fstat);

            if (!S_ISREG(fstat.st_mode)) {
                dfs_log(LL_SYSINFO) << "Encountered directory at: " << filepath << ", continuing...";
                continue;
            }

            dfs_log(LL_SYSINFO) << "File encountered at: " << filepath << ", adding to hash map";
            this->rw_map[dir_entry] = make_unique<shared_timed_mutex>();
        }
        closedir(dir);
        /* ----------------------------------------------------------------------------- */
    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {
        dfs_log(LL_DEBUG2) << "ProcessCallback invoked with request: " << request->name();
        Status status = this->CallbackList(context, request, response);
        if (!status.ok()) {
            dfs_log(LL_ERROR) << "Status not ok: " << status.error_message();
        }
        else {
            dfs_log(LL_DEBUG2) << "ProcessCallback succeeded";
        }
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {
            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    Status GetLock(ServerContext* context, const FileName* request, WriteLock* response) override {
        string client_id = request->client_id();
        string filename = request->name();


        if (context->IsCancelled()) {
            dfs_log(LL_ERROR) << "Deadline expired...";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
        }


        file_to_cid_mutex.lock();
        auto other_client_id_tuple = this->file_to_cid_map.find(filename);
        string other_client_id;

        if (other_client_id_tuple != this->file_to_cid_map.end()) {
            other_client_id = other_client_id_tuple->second;
        } else {
            other_client_id = "Empty id";
        }

        // If another file has write locked this file. 
        if (other_client_id.compare("Empty id") != 0 && client_id.compare(other_client_id) != 0) { // Other file holds lock
            file_to_cid_mutex.unlock();
            dfs_log(LL_ERROR) << "Could not aquire lock for: " << filename << ", lock already held";
            dfs_log(LL_ERROR) << "Other client with this id holding lock: " << other_client_id << " Your client id: " << client_id;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Lock already held");
        } else if (other_client_id.compare(client_id) == 0) { // Client already holds lock
            file_to_cid_mutex.unlock();
            dfs_log(LL_SYSINFO) << "Client: " << client_id << "already holds lock for file: " << filename;
        } else { // Adding lock for client
            file_to_cid_map[filename] = client_id;
            file_to_cid_mutex.unlock();
            this->AddFileMutex(filename);

        }

        dfs_log(LL_SYSINFO) << "Succesfully aquired lock for file: " << filename;
        return Status::OK;
    }


    Status Store(ServerContext* context, ServerReader<FileData>* reader, FileAck* response) override {
        FileData filedata;

        reader->Read(&filedata);
        string filename = filedata.name();
        uint32_t crc = filedata.crc();
        string client_id = filedata.client_id();
        long mtime = filedata.mtime();
        string filepath = WrapPath(filename);

        // File must have write lock to do this
        file_to_cid_mutex.lock_shared(); // Shared lock because we are only reading cid map.
        auto file_client_tuple = file_to_cid_map.find(filename);
        string prev_client_id;

        if (file_client_tuple != file_to_cid_map.end()) {
            prev_client_id = file_client_tuple->second;
        } else {
            prev_client_id = "Empty id";
        }

        dfs_log(LL_SYSINFO) << "About to store file: " << filename << ", Client id: " << client_id;


        // To store, client must have lock for file. If that's true, there will be an entry in the 
        // file_to_cid map with the client's client_id. Client must call GetLock before Store!
        if (prev_client_id.compare("Empty id") == 0) { // No entry -> client does not have write lock
            file_to_cid_mutex.unlock_shared();
            dfs_log(LL_ERROR) << "Client does not have write lock for this file: " << filename << ", server cannot store";
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Client doesn't have lock");
        } else if (prev_client_id.compare(client_id) != 0) { // Write lock is already held by another client. 
            file_to_cid_mutex.unlock_shared();
            dfs_log(LL_ERROR) << "Write lock is held by another client with id: " << prev_client_id << ", Your id: " << client_id;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Lock held by another client");
        }
        file_to_cid_mutex.unlock_shared();
        dfs_log(LL_SYSINFO) << "Client has lock for file: " << filename << ", continuing with store procedure";
        
        // Get lock since we know file has it.
        shared_timed_mutex* file_mutex = GetFileMutex(filename);
        // Lock file
        file_mutex->lock();
        // Lock the directory.
        async_mutex.lock();

        // Verify checksum first. If crc of client and server mathch, file already there and we
        // need to update the last_modified time
        Status checksum_status = ChecksumsDiffer(crc, filepath);
        
        // Checksums are the same, must update so that if client is newer->do nothing
        // If server is newer -> overwrite with new file
        if (!checksum_status.ok()) {
            struct stat fstat;
            if (stat(filepath.c_str(), &fstat) != 0) {
                dfs_log(LL_ERROR) << "Stat error";
                return Status(StatusCode::INTERNAL, "stat error");
            }
            
            if (checksum_status.error_code() == StatusCode::ALREADY_EXISTS && mtime > fstat.st_mtime) {
                dfs_log(LL_SYSINFO) << "File already on server (with same contents) but client has a more recent update time";
                dfs_log(LL_SYSINFO) << "Updating server to client mtime";

                struct utimbuf ub;
                ub.modtime = mtime;
                utime(filepath.c_str(), &ub); // Update mtime for file on server/
            }
            // We're done now, we don't want to continue if the file w/ same crc is already on the server.
            ReleaseClientMutex(filename); // Remove filename->client_id mapping from table.
            file_mutex->unlock();
            async_mutex.unlock();

            return checksum_status;
        }

        dfs_log(LL_SYSINFO) << "Synchronization code passed, now storing file: " << filename << " at path: " << filepath << " for client: " << client_id;

        ofstream ofs;

        do {
            if (!ofs.is_open()) {
                ofs.open(filepath, ofstream::out | ofstream::app);
            }

            if (context->IsCancelled()) {
                ReleaseClientMutex(filename);
                file_mutex->unlock();
                async_mutex.unlock();

                dfs_log(LL_ERROR) << "Deadline expired...";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
            }

            auto buffer_data = filedata.data();
            ofs << buffer_data;
            dfs_log(LL_SYSINFO) << "Block of data of size " << buffer_data.length() << " was written";
        } while(reader->Read(&filedata));

        ofs.close();
        ReleaseClientMutex(filename);

        // Send the ack back to the client
        FileStatus filestat;

        // File not found
        if ( (statWrapper(&filestat, filepath) == -1) ) {
            file_mutex->unlock();
            async_mutex.unlock();
            dfs_log(LL_ERROR) << "File not found on this path: " << filepath;
            return Status(StatusCode::NOT_FOUND, "NOT_FOUND");
        }
        
        response->set_name(filename);

        Timestamp* last_modified = new Timestamp(filestat.last_modified());
        response->set_allocated_last_modified(last_modified);

        file_mutex->unlock();
        async_mutex.unlock();

        dfs_log(LL_SYSINFO) << "Response filename: " << response->name();
        dfs_log(LL_SYSINFO) << "Response last_modified " << last_modified;
        dfs_log(LL_SYSINFO) << "Server store successful, returning status OK";

        return Status::OK;
    }


    Status Fetch(ServerContext* context, const FileName* request, ServerWriter<FileData>* writer) override {
        string filename = request->name();
        uint32_t crc = request->crc();
        uint64_t mtime = request->mtime();
        string filepath = WrapPath(filename); 

        struct stat fs;
        FileData response;

        // File being fetched, add mutex for this file. SHould already be there but just to be safe.
        AddFileMutex(filename);
        shared_timed_mutex* file_mutex = GetFileMutex(filename);

        // Lock file mutex and check if file already exists.
        file_mutex->lock();

        // Get file info (size in particular)
        if (stat(filepath.c_str(), &fs) == -1) {
            file_mutex->unlock();
            dfs_log(LL_ERROR) << "File not found";
            return Status(StatusCode::NOT_FOUND, "NOT_FOUND");
        }

        Status checksum_status = ChecksumsDiffer(crc, filepath);
        
        // Checksums are the same, must update so that if client is newer->do nothing
        // If server is newer -> overwrite with new file
        if (!checksum_status.ok()) {
            if (checksum_status.error_code() == StatusCode::ALREADY_EXISTS && mtime > (uint64_t) fs.st_mtime) {
                dfs_log(LL_SYSINFO) << "File already on server (with same contents) but client has a more recent update time";
                dfs_log(LL_SYSINFO) << "Updating server to client mtime";

                struct utimbuf ub;
                ub.modtime = mtime;
                utime(filepath.c_str(), &ub); // Update mtime for file on server/
            }
            // We're done now, we don't want to continue if the file w/ same crc is already on the server.
            file_mutex->unlock();
            return checksum_status;
        }

        dfs_log(LL_SYSINFO) << "Fetching file " << filename; 

        file_mutex->unlock();
        file_mutex->lock_shared(); // Lock shared because we don't need exclusive access for fetch (read)

        int filesize = fs.st_size;
        ifstream ifs(filepath, ifstream::in);

        dfs_log(LL_SYSINFO) << "Sending file: " << filepath << " of size: " << filesize << " to client";
        int bytes_remaining = filesize;

        while(!ifs.eof() && bytes_remaining > 0) {
            char buffer[BUFSIZE];
            int send_size = min(bytes_remaining, BUFSIZE);

            if (context->IsCancelled()) {
                file_mutex->unlock_shared();
                dfs_log(LL_ERROR) << "Client's deadline exceeded";
                return Status(StatusCode::DEADLINE_EXCEEDED, "DEADLINE EXCEEDED");
            }

            ifs.read(buffer, send_size);
            response.set_data(buffer, send_size);
            writer->Write(response);

            bytes_remaining -= send_size;
            dfs_log(LL_SYSINFO) << "Wrote: " << send_size << ", Remain: " << bytes_remaining;
        }
        ifs.close();
        file_mutex->unlock_shared();
        
        dfs_log(LL_SYSINFO) << "Done. File " << filepath << " sent to client";
        return Status::OK;
    }


    Status Delete(ServerContext* context, const FileName* request, FileAck* response) override {
        FileStatus fs;
        const string& filename = request->name();
        const string& filepath = this->WrapPath(filename);
        const string& client_id = request->client_id();

        AddFileMutex(filename);
        shared_timed_mutex* file_mutex = GetFileMutex(filename);

        file_to_cid_mutex.lock_shared(); // Lock_shared cause just reading.
        auto prev_client_id_tuple = file_to_cid_map.find(filename);
        string prev_client_id;
        if (prev_client_id_tuple == file_to_cid_map.end()) {
            file_to_cid_mutex.unlock_shared();
            dfs_log(LL_ERROR) << "Unable to get write lock, file not in table";
            return Status(StatusCode::RESOURCE_EXHAUSTED, "File not in table");
        } else {
            prev_client_id = prev_client_id_tuple->second;
            if (prev_client_id.compare(client_id) != 0) {
                file_to_cid_mutex.unlock_shared();
                dfs_log(LL_ERROR) << "Unable to get write lock, held by another";
                return Status(StatusCode::RESOURCE_EXHAUSTED, "Held by another");
            }
        }
        file_to_cid_mutex.unlock_shared();

        dfs_log(LL_SYSINFO) << "Successfully got validation that client has permission to delete file: " << filename;
        
        // Lock both async and file mutexes.
        async_mutex.lock();
        file_mutex->lock();

        if (statWrapper(&fs, filepath) == -1) {
            UnlockAll(filename);

            dfs_log(LL_ERROR) << "File not found.";
            return Status(StatusCode::NOT_FOUND, "NOT_FOUND");
        }


        if (context->IsCancelled()) {
            UnlockAll(filename);

            dfs_log(LL_ERROR) << "Client's deadline exceeded";
            return Status(StatusCode::DEADLINE_EXCEEDED, "DEADLINE EXCEEDED");
        }


        if (remove(filepath.c_str()) != 0) {
            UnlockAll(filename);

            dfs_log(LL_ERROR) << "File remove failed with error: " << strerror(errno);
            return Status(StatusCode::CANCELLED, "CANCELLED");
        }

        UnlockAll(filename);

        response->set_name(filename);
        Timestamp* last_modified = new Timestamp(fs.last_modified());
        response->set_allocated_last_modified(last_modified);

        dfs_log(LL_SYSINFO) << "Deleted file: " << response->name() << " with last modify time: " << response->last_modified();

        return Status::OK;        
    }


    Status ListAll(ServerContext* context, const EmptyRequest* request, AllFiles* response) override {
        DIR *dir;
        struct dirent *ent;

        async_mutex.lock();

        // Open directory
        if ( (dir = opendir(this->mount_path.c_str())) == NULL) {
            dfs_log(LL_ERROR) << "Directory: " << this->mount_path << " not found.";
            return Status(StatusCode::CANCELLED, "Dir not found");
        }

        while ( (ent = readdir(dir)) != NULL) {

            string dirname = ent->d_name;
            string filepath = this->WrapPath(dirname);

            struct stat fs;
            stat(filepath.c_str(), &fs);

            // Skip over any directories
            if (!S_ISREG(fs.st_mode)) {
                dfs_log(LL_SYSINFO) << "Found directory, skipping over...";
                continue;
            }

            dfs_log(LL_SYSINFO) << "Adding file " << dirname <<  "from filepath: " << filepath << " to response";

            FileStatus fstat;
            if (statWrapper(&fstat, filepath) == -1) {
                async_mutex.unlock();
                dfs_log(LL_ERROR) << "Was unable to call stat on file " << dirname;
                return Status(StatusCode::NOT_FOUND, "NOT_FOUND");
            }

            FileStatus* fack = response->add_filestat(); // Add entry
            fack->set_name(dirname);
            Timestamp* last_modified = new Timestamp(fstat.last_modified());
            fack->set_allocated_last_modified(last_modified);
            Timestamp* created = new Timestamp(fstat.created());
            fack->set_allocated_created(created);
            fack->set_size(fstat.size());
        }
        closedir(dir);
        async_mutex.unlock();
        return Status::OK;
    }


    Status GetFileStatus(ServerContext* context, const FileName* request, FileStatus* response) override {
        // StatWrapper implements this.
        return Status::OK;
    }

    Status CallbackList(ServerContext* context, const FileName* request, AllFiles* response) override {
        EmptyRequest new_request;
        return ListAll(context, &new_request, response);
    }
};

/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}
