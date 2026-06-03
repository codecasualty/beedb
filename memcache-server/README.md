# Memcache Server

A simple Java-based memcache server project.

## Requirements

- Java
- Maven

## Run

```bash
mvn compile
mvn exec:java -Dexec.mainClass="com.memcache.Server"
```

The idea is to create a simple memcache server in java.
First we will start with a simple server that listens on a port and accepts connections.
Then we will create a simple client that connects to the server and sends a request.
Finally we will create a simple server that listens on a port and accepts connections.

We will start by writing the complete code in one file , make our code working and then we will break it down into smaller parts.
And then work on scalability part
https://stackoverflow.com/questions/2187626/how-to-create-a-basic-java-server


now we will start using socketchannel and serversocketchannel because
ServerSocket       -> Wait for clients (old blocking)
Socket             -> Connected client (old blocking)

ServerSocketChannel -> Wait for clients (NIO)
SocketChannel       -> Connected client (NIO/non-blocking)

we will accept the connection and then we will read the data from the client and write the response back to the client
for reading we will follow [length][data] format
for writing we will follow [length][data] format


now we will create cache and cacheitem
cache will be a concurrent hashmap
cacheitem will be a key value pair
we will use the cache to store the data
and we will use the cacheitem to store the data
Similarly we will have command and command type
command will be a key value pair
command type will be a enum
we will use the command to store the data
and we will use the command type to figure out the type of the command
and there will be command parser whose job is to parse the command


we will have basically 4 layers
network layer which will have -> server.java -> TCP sockets, threads and bytes in/out
protocol layer which will have -> command.java -> command type, command parser
data layer which will have -> cache.java -> cache, cache item
logic layer which will have -> handler.java -> command handler 



### How propose() Works — Full Explanation
propose() is the bridge between the memcache client world and the Raft world.

Without Raft (current code):


Client → "SET foo 0 0 3\r\nbar" → Server.java → CommandProcessor → Cache → "STORED"
With Raft:


Client → "SET foo 0 0 3\r\nbar" → Server.java → RaftNode.propose() → ??? → "STORED"
The problem: replication takes time (network round-trips). Server.java can't just sit and block the NIO thread waiting. So propose() returns a CompletableFuture<String> immediately — "I'll give you the result when it's ready."

The full propose() flow:


1. Server.java calls raftNode.propose("set foo 0 0 3\r\nbar")

2. RaftNode checks: am I the leader?
   NO  → complete future with error "NOT_LEADER node2"
         Server tells client to retry at node2
   YES → continue

3. Create a LogEntry(index=nextIndex, term=currentTerm, command="set foo...")
   Append it to local log
   Store a CompletableFuture in a map: pendingCommands.put(logIndex, future)

4. Send AppendEntries to all followers (async)

5. propose() returns the future immediately
   Server.java attaches a callback: future.thenAccept(response → send to client)

6. [Meanwhile, on a separate thread]
   Followers ACK
   Leader advances commitIndex
   applyCommittedEntries() loop picks up the entry
   Calls CommandProcessor.process(command, cache)
   Gets back "STORED"
   Completes the future: pendingCommands.get(logIndex).complete("STORED")

7. Server.java's callback fires → sends "STORED" to client
The pendingCommands map is the key piece:


// in RaftNode
private Map<Integer, CompletableFuture<String>> pendingCommands = new HashMap<>();

// in propose():
int index = log.lastIndex() + 1;
CompletableFuture<String> future = new CompletableFuture<>();
pendingCommands.put(index, future);
log.append(new LogEntry(index, command, currentTerm, false));
// send AppendEntries to peers...
return future;

// in applyCommittedEntries():
String result = CommandProcessor.process(command, cache).toProtocolString();
CompletableFuture<String> future = pendingCommands.remove(lastApplied);
if (future != null) future.complete(result);
The future ties the client's waiting request to the specific log index. When that index gets committed and applied, the future completes and the client gets their response.