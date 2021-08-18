
# File Server with gRPC

## Design Before Cache

### Design
As far as design goes, there wasn't really too many decisions to be made here. I just needed to implement the given RPC calls. The RPC calls themselves are the ones that are specified but the actual messages I chose as follows: 

- FileData: A message type that acts as the data buffer which contains a file name field and a bytes field containing the raw bytesteam chunk. This is used both in Store and Fetch where file data must potentially be sent over the RPC call. 
- FileName: Just contains the name. The name is the key for many operations i.e. Fetch(FileName) simply fetches the file with the name specified by the message. Nothing more is needed in the request.
- FileAck: This just contains a bonus timestamp as well as name.
- AllFiles: This contains a list of FileAcks and is used for the ListAll RPC call.
- FileStatus: This is called accompanied with stat() to fill out the metadata that's required. This was more relevant in part 2 than part1. 

As far as the actual implementation for the RPC calls, it was pretty standard. The file transfer made use of ifstream and ofstream. A wrapper was created around stat() to get a message type that held this info and keep track of modify times, etc. Again, this was more relevant in part 2 than part 1.

### Flow of Control
1. Server starts up with various arguments
2. Client is started with a specific command (i.e. Store)
3. Client node calls the specific method. This issues the RPC call via the service stub. 
4. Server node unpacks the call with relevant data and performs the corresponding function and logic. 
5. The response is sent back to the client node. If succesful, this is the end of this particular interaction and the server continues to listen.

### Implementation and Testing
I implemented the code in increments. First Store, then Fetch, etc on both client and server side. I tested to make sure each call works before moving onto the next. I also made a lot of use from the logging functionality provided, this aided in debugging and testing. If time permitted, I would have written some unit and integration tests.  


## Design After Cache

### Design
This part was a lot more involved than without cache for sure. The trickiest part was the synchronization. For this, two maps were used to keep track of things. First a map that stores filename to client_id. This keeps track of which files are associated with which clients at any given time. If the file is not in this map, that means it is not associated with any client and can be picked up. The second map is the actual filename to mutex mapping which contains the mutex that the file owns. Unlike filenam to cid map, this mapping shouldn't really change much apart from adding new entries. Clients will lock this mutex when they wish to perform any kind of write operations (delete, store, etc.). 

The next thing which differs significantly from part 1 is the async threads. First there is inotify. As far as I can tell, I didn't really do anything for this apart from guarding it's callback with a mutex so the other async thread didn't execute at the same time (causing potential race conditions). The signficant async thread is the one that gets the list of file statuses from the server and then performs the necessary logic to keep the directories in sync. The logic is as follows:

- If the file is not on the server, we need to fetch it from the server.
- Else if the file exists locally but the server timestamp is more recent, we still need to fetch so our file is up to date.
- Else if file exists and servers client time stamp is more recent then we need to store it on the server.
- Else we do nothing, the files are already in sync. 

On the server side, we simply implement the callbacks to call AllFiles which is pretty straightforward. That sends the file statuses back to the client so it can be handled as described above.

Everything else is more or less the same as part 1 except guarded by synchronization constructs. 

### Flow of Control
Can be used like part 1 but I'll talk about the flow of control unique to part 2:

1. Server starts up
2. Client starts up and mounts directory.
3. The directories are immediately synchronized: If client does not have files that the server has, it fetches them. Likewise, if the server sees the client has made any modification to the files and is notified by inotify, it update itself. 
4. This continues on forever.




