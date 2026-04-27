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