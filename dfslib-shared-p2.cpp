#include <string>
#include <iostream>
#include <fstream>
#include <cstddef>
#include <sys/stat.h>

#include "dfslib-shared-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using namespace dfs_service;
using namespace std;

using google::protobuf::util::TimeUtil;
using google::protobuf::Timestamp;

// Global log level used throughout the system
// Note: this may be adjusted from the CLI in
// both the client and server executables.
// This shouldn't be changed at the file level.
dfs_log_level_e DFS_LOG_LEVEL = LL_ERROR;

//
// STUDENT INSTRUCTION:
//
// Add your additional code here. You may use
// the shared files for anything you need to add outside
// of the main structure, but you don't have to use them.
//
// Just be aware they are always submitted, so they should
// be compilable.
//

int statWrapper(FileStatus* filestat, string mount_path) {
    struct stat buf;
    if (stat(mount_path.c_str(), &buf) == -1) {
        // File not found
        return -1;
    }

    // Time of last status change
    Timestamp* created = new Timestamp(TimeUtil::TimeTToTimestamp(buf.st_ctime));
    // Time of last data modification 
    Timestamp* last_modified = new Timestamp(TimeUtil::TimeTToTimestamp(buf.st_mtime));

    // Fill our protobuf with the correct data for the file status.
    filestat->set_name(mount_path);
    filestat->set_size(buf.st_size);
    filestat->set_allocated_created(created);
    filestat->set_allocated_last_modified(last_modified);

    // Same return types as stat() since we just want to wrap it.
    return 0;
}

