# Redis Clone in Go

A Redis-inspired in-memory key-value store built from scratch using **Go**. This project is a hands-on learning project focused on understanding how Redis-like servers work internally, including TCP networking, RESP protocol parsing, command handling, concurrency, key expiration, and persistence.

## Features

- **TCP Server** — Accepts client connections and handles multiple clients.
- **RESP Protocol** — Parses Redis Serialization Protocol (RESP) array and bulk-string requests.
- **Key-Value Storage** — Stores and retrieves string values using keys.
- **Command Support** — Implements common Redis-style commands.
- **Key Expiration** — Supports expiring keys and checking their remaining lifetime.
- **Persistence** — Saves the store to a JSON snapshot and restores it when the server restarts.
- **Concurrency Safety** — Uses a mutex to protect shared data structures.
- **Testing** — Includes tests for command behavior, persistence, and rollback-related functionality.

## Supported Commands

| Command | Description |
|---|---|
| `PING` | Checks whether the server is responding |
| `SET` | Stores a key-value pair |
| `GET` | Retrieves a value by key |
| `DEL` | Deletes one or more keys |
| `EXISTS` | Checks whether a key exists |
| `INCR` | Increments an integer value |
| `INCRBY` | Increments a value by a specified amount |
| `DECR` | Decrements an integer value |
| `DECRBY` | Decrements a value by a specified amount |
| `MSET` | Sets multiple key-value pairs |
| `MGET` | Retrieves multiple values |
| `APPEND` | Appends text to an existing value |
| `STRLEN` | Returns the length of a string value |
| `GETSET` | Replaces a value and returns the previous value |
| `SETNX` | Sets a key only if it does not already exist |
| `GETDEL` | Retrieves a value and deletes its key |
| `SETEX` | Sets a value with an expiration time |
| `TTL` | Returns the remaining lifetime of a key |
| `EXPIRE` | Sets an expiration time on a key |
| `PERSIST` | Removes a key's expiration |
| `TYPE` | Returns the type of a key |

## Tech Stack

- **Language:** Go
- **Networking:** TCP sockets
- **Protocol:** RESP-style request parsing
- **Storage:** Go maps
- **Persistence:** JSON snapshots
- **Testing:** Go testing tools

## Getting Started

### Prerequisites

- Go installed on your machine
- Netcat (`nc`) for sending test commands from the terminal

### 1. Clone the repository

```bash
git clone https://github.com/nt063574-ship-it/redis-clone
cd redis-clone
```



### 2. Start the server

```bash
go run .
```

The server listens on **port 6380** in the current development configuration.

### 3. Test the server

You can send RESP-formatted requests using `nc`.

**Test PING**

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc -w 2 127.0.0.1 6380
```

Expected response:

```text
+PONG
```

**Test SET**

```bash
printf '*3\r\n$3\r\nSET\r\n$5\r\nfruit\r\n$5\r\napple\r\n' | nc -w 2 127.0.0.1 6380
```

Expected response:

```text
+OK
```

**Test GET**

```bash
printf '*2\r\n$3\r\nGET\r\n$5\r\nfruit\r\n' | nc -w 2 127.0.0.1 6380
```

Expected response:

```text
$5
apple
```

Run the server in one terminal and the test commands in another.

## Persistence

The project uses a JSON snapshot to save key-value data and restore it when the server starts again.

This allows stored data to survive a server restart. Expired keys are also handled during startup so that keys whose expiration has passed are not restored as valid entries.

## Running Tests

Run the project's test suite:

```bash
go test ./...
```

Run tests with the Go race detector:

```bash
go test -race ./...
```

Run static checks:

```bash
go vet ./...
```

## What I'm Learning

The main goal of this project is to understand how a Redis-like server works under the hood rather than relying on an existing Redis server implementation.

Through this project, I'm learning about:

- TCP client-server communication
- RESP request parsing and response formatting
- Command parsing and execution
- In-memory data structures
- Concurrency and mutexes
- Key expiration and TTL handling
- Data persistence across restarts
- Testing and debugging network servers

## Current Status

This project is a work in progress. Core commands, expiration handling, and persistence are implemented, while protocol hardening and reliability improvements are ongoing.

It is intended for learning and experimentation, not as a production replacement for Redis.

## Future Improvements

- Strengthen RESP protocol validation and malformed-request handling
- Improve persistence reliability and error handling
- Add more automated integration tests
- Improve resource limits and connection handling
- Expand command compatibility

## Acknowledgements

Inspired by Redis and built as a learning project to explore systems programming concepts with Go.
