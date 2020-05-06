#ifndef PR4_DFSLIB_CLIENTNODE_H
#define PR4_DFSLIB_CLIENTNODE_H

#include <string>
#include <vector>
#include <map>
#include <limits.h>
#include <chrono>
#include <mutex>

#include <grpcpp/grpcpp.h>

#include "src/dfslibx-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

class DFSClientNodeP2 : public DFSClientNode {

private:
    mutable std::mutex async_mutex;

public:
    DFSClientNodeP2();

    ~DFSClientNodeP2();

    /**
     * Request write access to the server
     *
     * @param filename
     * @return bool
     */
    grpc::StatusCode RequestWriteAccess(const std::string& filename) override ;

    /**
     * Store a file from the mount path on to the RPC server
     *
     * @param filename
     * @return grpc::StatusCode
     */
    grpc::StatusCode Store(const std::string& filename) override ;

    /**
     * Fetch a file from the RPC server and put it in the mount path
     *
     * @param filename
     * @return grpc::StatusCode
     */
    grpc::StatusCode Fetch(const std::string& filename) override ;

    /**
     * Delete a file from the RPC server
     *
     * @param filename
     * @return grpc::StatusCode
     */
    grpc::StatusCode Delete(const std::string& filename) override ;

    /**
     * Get or print a list from the RPC server.
     *
     * @param file_map
     * @param display
     * @return grpc::StatusCode
     */
    grpc::StatusCode List(std::map<std::string,int>* file_map = NULL, bool display = false) override;

    /**
     * Get or print the status details for a given filename,
     *
     * You won't be tested on this method, but you will most likely find
     * that you need to implement it in order to communicate file status
     * between the server and the client.
     *
     *
     * @param filename
     * @param file_status
     * @return grpc::StatusCode
     */
    grpc::StatusCode Stat(const std::string& filename, void* file_status = NULL) override;

    /**
     * Handle the asynchronous callback list completion queue
     *
     * This method gets called whenever an asynchronous callback
     * message is received from the server.
     */
    void HandleCallbackList();

    /**
     * Initialize the callback list
     */
     void InitCallbackList() override;

    /**
     * Watcher wrapper
     *
     * This method will get called whenever inotify signals
     * a change to a file. The callback parameter is a method
     * that should be called at the time this method is called.
     *
     *
     *
     * @param callback
     *
     */
    void InotifyWatcherCallback(std::function<void()> callback) override;

};
#endif
