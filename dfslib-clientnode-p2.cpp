#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include <google/protobuf/util/time_util.h>

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using namespace dfs_service;
using namespace std;
using google::protobuf::util::TimeUtil;
using std::chrono::system_clock;
using std::chrono::milliseconds;


extern dfs_log_level_e DFS_LOG_LEVEL;


using FileRequestType = FileName;
using FileListResponseType = AllFiles;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {
    ClientContext context;
    FileName request;
    WriteLock response;
    
    request.set_name(filename);
    request.set_client_id(ClientId());

    auto deadline = system_clock::now() + milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);

    Status status = this->service_stub->GetLock(&context, request, &response);
    if (!status.ok()) {
        dfs_log(LL_ERROR) << "Failed to get lock: " << status.error_message();
        return status.error_code();
    }

    dfs_log(LL_SYSINFO) << "Client: " << this->client_id << " got lock for file: " << filename;
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {
    ClientContext context;
    FileAck response;
    FileData request;
    const string& filepath = WrapPath(filename);
    struct stat fs;

    // Set deadline
    auto deadline = system_clock::now() + milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);

    // Get file info (size in particular)
    if (stat(filepath.c_str(), &fs) == -1) {
        dfs_log(LL_ERROR) << "File not found";
        return StatusCode::NOT_FOUND;
    }

    StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status != StatusCode::OK) {
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    request.set_name(filename);
    request.set_client_id(ClientId());
    request.set_crc(dfs_file_checksum(filepath, &crc_table));
    request.set_mtime((long int) fs.st_mtime);
    int filesize = fs.st_size;

    unique_ptr<ClientWriter<FileData>> writer(this->service_stub->Store(&context, &response));

    ifstream ifs(filepath, ifstream::in);
    dfs_log(LL_SYSINFO) << "Writing file: " << filepath << " of size: " << filesize << " to server";

    int bytes_remaining = filesize;
    while(!ifs.eof() && bytes_remaining > 0) {
        char buffer[BUFSIZE];
        int send_size = min(bytes_remaining, BUFSIZE);
        ifs.read(buffer, send_size);

        request.set_data(buffer, send_size);
        writer->Write(request);

        bytes_remaining -= send_size;
        dfs_log(LL_SYSINFO) << "Wrote: " << send_size << ", Remaining: " << bytes_remaining;
    }
    ifs.close();
    dfs_log(LL_SYSINFO) << "Finished writing, bytes remaining: " << bytes_remaining;

    writer->WritesDone();
    Status status = writer->Finish();
    if (!status.ok()) {
        dfs_log(LL_SYSINFO) << "POSSIBLE ERROR: " << status.error_message() << ", code:" << status.error_code();
    }

    dfs_log(LL_SYSINFO) << "Done. Debug string from Store client response: " << response.DebugString();
    return status.error_code();   
}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {
    ClientContext context;
    FileName request;
    FileData response;
    const string& filepath = this->WrapPath(filename);

    auto deadline = system_clock::now() + milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);

    struct stat fs;
    if (stat(filepath.c_str(), &fs) == 0) {
        dfs_log(LL_SYSINFO) << "File is already on client";
        request.set_mtime( (uint64_t) fs.st_mtime);
    }
    else {
        request.set_mtime(0);
    }
    request.set_name(filename);
    uint32_t client_crc = dfs_file_checksum(filepath, &this->crc_table);
    request.set_crc(client_crc);
    dfs_log(LL_SYSINFO) << "CRC checksum for file: " << filename << " is " << client_crc;
    dfs_log(LL_SYSINFO) << "File path for file: " << filename << " is " << filepath;
    unique_ptr<ClientReader<FileData>> reader(this->service_stub->Fetch(&context, request));

    ofstream ofs;
    while(reader->Read(&response)) {
        if (!ofs.is_open()) {
            ofs.open(filepath, ofstream::out | ofstream::app);
        }
        
        auto buffer_data = response.data();
        ofs << buffer_data;
        dfs_log(LL_SYSINFO) << "Block of data of size " << buffer_data.length() << " was written to disk";
    }
    ofs.close();
    
    Status status = reader->Finish();
    if (!status.ok()) {
        dfs_log(LL_ERROR) << "ERROR: " << status.error_message() << ", code:" << status.error_code();
    }

    return status.error_code();
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {
    ClientContext context;
    FileName request;
    FileAck response;

    StatusCode status_code = RequestWriteAccess(filename);
    if (status_code != StatusCode::OK) {
        dfs_log(LL_ERROR) << "FAiled to get lock...";
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    auto deadline = system_clock::now() + milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);
    request.set_name(filename);
    request.set_client_id(ClientId());

    Status status = this->service_stub->Delete(&context, request, &response);

    if (!status.ok()) {
        dfs_log(LL_ERROR) << "ERROR: " << status.error_message() << ", code:" << status.error_code();
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Deleted file succesfully: " << response.name();
    return status.error_code();
}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {
    ClientContext context;
    EmptyRequest request;
    AllFiles response;

    auto deadline = system_clock::now() + milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);

    Status status = this->service_stub->ListAll(&context, request, &response);
    if (!status.ok()) {
        dfs_log(LL_ERROR) << "ERROR: " << status.error_message() << ", code:" << status.error_code();
        return status.error_code();
    }

    dfs_log(LL_SYSINFO) << "Got files succesfully";

    for (FileStatus fack : response.filestat()) {
        int time = TimeUtil::TimestampToSeconds(fack.last_modified());
        auto filename = fack.name();
        file_map->insert(pair<string, int>(filename, time));
    }

    if (display) {
        for (auto elem : *file_map) {
            cout << "Filename: " << elem.first << " Last modified: " << elem.second << endl;
        }
    }
    return status.error_code();  
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {
    return StatusCode::OK;
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {
    async_mutex.lock();
    callback();
    dfs_log(LL_SYSINFO) << "Inotify called.";
    async_mutex.unlock();
}

void DFSClientNodeP2::HandleCallbackList() {
    void* tag;
    bool ok = false;

    while (completion_queue.Next(&tag, &ok)) {
        {
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                async_mutex.lock();

                for (const FileStatus& server_filestat : call_data->reply.filestat()) {
                    FileStatus client_filestat;
                    string filepath = WrapPath(server_filestat.name());
                    
                    // If file does not exist on client, fetch from server
                    if (statWrapper(&client_filestat, filepath) != 0) {
                        dfs_log(LL_SYSINFO) << "File does not exist on client, fetching: " << server_filestat.name() << " from server.";
                        StatusCode code = Fetch(server_filestat.name());
                        if (code != StatusCode::OK) {
                            dfs_log(LL_ERROR) << "Fetch failed..." << " with code: " << code;
                        }
                    // If file exists but server timestamp is more recent than cients, we must still fetch
                    } else if (server_filestat.last_modified() > client_filestat.last_modified()) {
                        dfs_log(LL_SYSINFO) << "Server timestamp more recent, fetching...";
                        dfs_log(LL_SYSINFO) << "Server timestamp is: " << server_filestat.last_modified() << "Client timestamp is: " << client_filestat.last_modified();
                        StatusCode code = Fetch(server_filestat.name());
                        if (code == StatusCode::ALREADY_EXISTS) {
                            time_t server_mtime = TimeUtil::TimestampToTimeT(server_filestat.last_modified());
                            struct utimbuf ub;
                            ub.modtime = server_mtime;
                            utime(filepath.c_str(), &ub);
                        } else if (code != StatusCode::OK) {
                            dfs_log(LL_ERROR) << "Fetch failed...";
                        } else {
                            dfs_log(LL_ERROR) << "Fetch somehow returned OK even though file already on client.";
                        }
                    // File exists but client timesamp is more recent than server timestamp -> Store it.
                    } else if (server_filestat.last_modified() < client_filestat.last_modified()){
                        dfs_log(LL_SYSINFO) << "Client time stamp more recent than servers, storing on server";
                        dfs_log(LL_SYSINFO) << "Server timestamp is: " << server_filestat.last_modified() << "Client timestamp is: " << client_filestat.last_modified();
                        StatusCode code = Store(server_filestat.name());
                        if (code != StatusCode::OK && code != StatusCode::ALREADY_EXISTS) {
                            dfs_log(LL_ERROR) << "Store failed...";
                        }
                    }
                }
                async_mutex.unlock();

                // Note that client never deletes a file if server doen't have it. Only the opposite can happen
                // i.e. server deletes a file if it sees that client no longer has it.

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;
        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}


