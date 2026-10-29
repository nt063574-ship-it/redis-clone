package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxArrayElements = 1000
	maxBulkStringLen = 1024 * 1024
	maxHeaderLen     = 64 * 1024
)

func handleConnection(conn net.Conn, KeyValueStore map[string]string, mu *sync.Mutex, ExpiryStore map[string]time.Time) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, maxHeaderLen)

	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from connection:", err)
			}
			return
		}

		if len(line) > maxHeaderLen || len(line) < 3 ||
			line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
			_, _ = conn.Write([]byte("-ERR invalid RESP array header\r\n"))
			return
		}

		lineTrim := string(line[:len(line)-2])
		if !strings.HasPrefix(lineTrim, "*") {
			_, _ = conn.Write([]byte("-ERR invalid array header\r\n"))
			return
		}

		arrayLength, err := strconv.Atoi(lineTrim[1:])
		if err != nil || arrayLength < 1 || arrayLength > maxArrayElements {
			_, _ = conn.Write([]byte("-ERR invalid array length\r\n"))
			return
		}

		args := make([]string, 0, arrayLength)
		nullArgs := make([]bool, 0, arrayLength)

		for i := 0; i < arrayLength; i++ {
			nextLine, err := reader.ReadSlice('\n')
			if err != nil {
				_, _ = conn.Write([]byte("-ERR incomplete RESP command\r\n"))
				return
			}

			if len(nextLine) > maxHeaderLen || len(nextLine) < 3 ||
				nextLine[len(nextLine)-2] != '\r' || nextLine[len(nextLine)-1] != '\n' {
				_, _ = conn.Write([]byte("-ERR invalid bulk-string header\r\n"))
				return
			}

			nextLineTrim := string(nextLine[:len(nextLine)-2])
			if !strings.HasPrefix(nextLineTrim, "$") {
				_, _ = conn.Write([]byte("-ERR expected bulk string\r\n"))
				return
			}

			bulkLength, err := strconv.Atoi(nextLineTrim[1:])
			if err != nil || bulkLength < -1 || bulkLength > maxBulkStringLen {
				_, _ = conn.Write([]byte("-ERR invalid bulk-string length\r\n"))
				return
			}

			if bulkLength == -1 {
				args = append(args, "")
				nullArgs = append(nullArgs, true)
				continue
			}

			buffer := make([]byte, bulkLength)
			if _, err = io.ReadFull(reader, buffer); err != nil {
				_, _ = conn.Write([]byte("-ERR incomplete bulk string\r\n"))
				return
			}

			firstByte, err := reader.ReadByte()
			if err != nil {
				_, _ = conn.Write([]byte("-ERR incomplete bulk-string terminator\r\n"))
				return
			}

			secondByte, err := reader.ReadByte()
			if err != nil {
				_, _ = conn.Write([]byte("-ERR incomplete bulk-string terminator\r\n"))
				return
			}

			if firstByte != '\r' || secondByte != '\n' {
				_, _ = conn.Write([]byte("-ERR invalid bulk-string terminator\r\n"))
				return
			}

			args = append(args, string(buffer))
			nullArgs = append(nullArgs, false)
		}

		if len(args) == 0 || nullArgs[0] {
			_, _ = conn.Write([]byte("-ERR command cannot be null\r\n"))
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			if len(args) > 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'PING' command\r\n"))
			} else if len(args) == 2 {
				if nullArgs[1] {
					_, _ = conn.Write([]byte("-ERR PING message cannot be null\r\n"))
				} else {
					_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(args[1]), args[1])
				}
			} else {
				_, _ = conn.Write([]byte("+PONG\r\n"))
			}

		case "SET":
			if len(args) != 3 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
				continue
			}
			if nullArgs[1] || nullArgs[2] {
				_, _ = conn.Write([]byte("-ERR SET arguments cannot be null\r\n"))
				continue
			}

			mu.Lock()
			KeyValueStore[args[1]] = args[2]
			delete(ExpiryStore, args[1])
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
				continue
			}
			_, _ = conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(args) != 2 || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key for 'GET'\r\n"))
				continue
			}

			mu.Lock()
			value, exists := KeyValueStore[args[1]]
			if exists {
				expiryTime, hasExpiry := ExpiryStore[args[1]]
				if hasExpiry && !time.Now().Before(expiryTime) {
					delete(KeyValueStore, args[1])
					delete(ExpiryStore, args[1])
					exists = false
					if err := saveSnapshot(KeyValueStore, ExpiryStore); err != nil {
						mu.Unlock()
						_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
						continue
					}
				}
			}
			mu.Unlock()

			if exists {
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(value), value)
			} else {
				_, _ = conn.Write([]byte("$-1\r\n"))
			}

		case "DEL":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\r\n"))
				continue
			}
			if len(args) > maxArrayElements {
				_, _ = conn.Write([]byte("-ERR too many arguments\r\n"))
				continue
			}
			for i := 1; i < len(args); i++ {
				if nullArgs[i] {
					_, _ = conn.Write([]byte("-ERR DEL key cannot be null\r\n"))
					goto nextCommand
				}
			}

			mu.Lock()
			deleted := 0
			now := time.Now()
			for _, key := range args[1:] {
				if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !now.Before(expiry) {
					delete(KeyValueStore, key)
					delete(ExpiryStore, key)
					continue
				}
				if _, exists := KeyValueStore[key]; exists {
					delete(KeyValueStore, key)
					delete(ExpiryStore, key)
					deleted++
				}
			}
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = fmt.Fprintf(conn, ":%d\r\n", deleted)
			}

		case "EXISTS":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'EXISTS' command\r\n"))
				continue
			}
			for i := 1; i < len(args); i++ {
				if nullArgs[i] {
					_, _ = conn.Write([]byte("-ERR EXISTS key cannot be null\r\n"))
					goto nextCommand
				}
			}

			mu.Lock()
			count := 0
			now := time.Now()
			for _, key := range args[1:] {
				if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !now.Before(expiry) {
					delete(KeyValueStore, key)
					delete(ExpiryStore, key)
					continue
				}
				if _, exists := KeyValueStore[key]; exists {
					count++
				}
			}
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = fmt.Fprintf(conn, ":%d\r\n", count)
			}

		case "INCR", "INCRBY", "DECRBY":
			expectedArgs := 2
			if cmd != "INCR" {
				expectedArgs = 3
			}
			if len(args) != expectedArgs || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key\r\n"))
				continue
			}

			delta := 1
			if cmd != "INCR" {
				if nullArgs[2] {
					_, _ = conn.Write([]byte("-ERR increment cannot be null\r\n"))
					continue
				}
				delta, err = strconv.Atoi(args[2])
				if err != nil {
					_, _ = conn.Write([]byte("-ERR increment is not an integer\r\n"))
					continue
				}
				if cmd == "DECRBY" {
					if delta == math.MinInt {
						_, _ = conn.Write([]byte("-ERR increment or decrement would overflow\r\n"))
						continue
					}
					delta = -delta
				}
			}

			mu.Lock()
			key := args[1]
			current := 0
			exists := false

			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
			} else if value, ok := KeyValueStore[key]; ok {
				current, err = strconv.Atoi(value)
				if err != nil {
					mu.Unlock()
					_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}
				exists = true
			}

			if (delta > 0 && current > math.MaxInt-delta) ||
				(delta < 0 && current < math.MinInt-delta) {
				mu.Unlock()
				_, _ = conn.Write([]byte("-ERR increment or decrement would overflow\r\n"))
				continue
			}

			result := current + delta
			KeyValueStore[key] = strconv.Itoa(result)
			if !exists {
				delete(ExpiryStore, key)
			}
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = fmt.Fprintf(conn, ":%d\r\n", result)
			}

		case "MSET":
			if len(args) < 3 || len(args)%2 == 0 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'MSET' command\r\n"))
				continue
			}

			valid := true
			for i := 1; i < len(args); i += 2 {
				if nullArgs[i] || nullArgs[i+1] {
					valid = false
					break
				}
			}
			if !valid {
				_, _ = conn.Write([]byte("-ERR MSET arguments cannot be null\r\n"))
				continue
			}

			mu.Lock()
			for i := 1; i < len(args); i += 2 {
				KeyValueStore[args[i]] = args[i+1]
				delete(ExpiryStore, args[i])
			}
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}

		case "MGET":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'MGET' command\r\n"))
				continue
			}

			mu.Lock()
			now := time.Now()
			var response strings.Builder
			for _, key := range args[1:] {
				if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !now.Before(expiry) {
					delete(KeyValueStore, key)
					delete(ExpiryStore, key)
					response.WriteString("$-1\r\n")
					continue
				}
				if nullArgs[len(response.String())] {
					// Null keys are treated as missing values.
				}
				if value, exists := KeyValueStore[key]; exists {
					fmt.Fprintf(&response, "$%d\r\n%s\r\n", len(value), value)
				} else {
					response.WriteString("$-1\r\n")
				}
			}
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(args)-1) + response.String()))
			}

		case "APPEND":
			if len(args) != 3 || nullArgs[1] || nullArgs[2] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null argument for 'APPEND'\r\n"))
				continue
			}

			mu.Lock()
			key := args[1]
			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
			}
			KeyValueStore[key] += args[2]
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			length := len(KeyValueStore[key])
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = fmt.Fprintf(conn, ":%d\r\n", length)
			}

		case "STRLEN":
			if len(args) != 2 || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key for 'STRLEN'\r\n"))
				continue
			}

			mu.Lock()
			value, exists := KeyValueStore[args[1]]
			if expiry, hasExpiry := ExpiryStore[args[1]]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, args[1])
				delete(ExpiryStore, args[1])
				exists = false
			}
			mu.Unlock()

			if exists {
				_, _ = fmt.Fprintf(conn, ":%d\r\n", len(value))
			} else {
				_, _ = conn.Write([]byte(":0\r\n"))
			}

		case "GETSET":
			if len(args) != 3 || nullArgs[1] || nullArgs[2] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null argument for 'GETSET'\r\n"))
				continue
			}

			mu.Lock()
			key := args[1]
			oldValue, exists := KeyValueStore[key]
			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
				exists = false
			}
			KeyValueStore[key] = args[2]
			delete(ExpiryStore, key)
			err := saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else if exists {
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(oldValue), oldValue)
			} else {
				_, _ = conn.Write([]byte("$-1\r\n"))
			}

		case "SETNX":
			if len(args) != 3 || nullArgs[1] || nullArgs[2] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null argument for 'SETNX'\r\n"))
				continue
			}

			mu.Lock()
			key := args[1]
			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
			}
			_, exists := KeyValueStore[key]
			var err error
			if !exists {
				KeyValueStore[key] = args[2]
				err = saveSnapshot(KeyValueStore, ExpiryStore)
			}
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else if exists {
				_, _ = conn.Write([]byte(":0\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}

		case "GETDEL":
			if len(args) != 2 || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key for 'GETDEL'\r\n"))
				continue
			}

			mu.Lock()
			key := args[1]
			value, exists := KeyValueStore[key]
			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
				exists = false
			}
			var err error
			if exists {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
				err = saveSnapshot(KeyValueStore, ExpiryStore)
			}
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else if exists {
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(value), value)
			} else {
				_, _ = conn.Write([]byte("$-1\r\n"))
			}

		case "SETEX":
			if len(args) != 4 || nullArgs[1] || nullArgs[2] || nullArgs[3] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null argument for 'SETEX'\r\n"))
				continue
			}

			expirySeconds, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil || expirySeconds <= 0 || expirySeconds > int64(math.MaxInt64/int64(time.Second)) {
				_, _ = conn.Write([]byte("-ERR invalid expiration time\r\n"))
				continue
			}

			mu.Lock()
			KeyValueStore[args[1]] = args[3]
			ExpiryStore[args[1]] = time.Now().Add(time.Duration(expirySeconds) * time.Second)
			err = saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}

		case "TTL":
			if len(args) != 2 || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key for 'TTL'\r\n"))
				continue
			}

			mu.Lock()
			_, exists := KeyValueStore[args[1]]
			if exists {
				if expiry, hasExpiry := ExpiryStore[args[1]]; hasExpiry {
					remaining := time.Until(expiry)
					if remaining <= 0 {
						delete(KeyValueStore, args[1])
						delete(ExpiryStore, args[1])
						err := saveSnapshot(KeyValueStore, ExpiryStore)
						mu.Unlock()
						if err != nil {
							_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
						} else {
							_, _ = conn.Write([]byte(":-2\r\n"))
						}
						continue
					}
					seconds := int64(remaining / time.Second)
					mu.Unlock()
					_, _ = fmt.Fprintf(conn, ":%d\r\n", seconds)
					continue
				}
				mu.Unlock()
				_, _ = conn.Write([]byte(":-1\r\n"))
			} else {
				mu.Unlock()
				_, _ = conn.Write([]byte(":-2\r\n"))
			}

		case "EXPIRE":
			if len(args) != 3 || nullArgs[1] || nullArgs[2] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null argument for 'EXPIRE'\r\n"))
				continue
			}

			expirySeconds, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				_, _ = conn.Write([]byte("-ERR invalid expiration time\r\n"))
				continue
			}

			mu.Lock()
			key := args[1]
			_, exists := KeyValueStore[key]
			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
				exists = false
			}

			if !exists {
				mu.Unlock()
				_, _ = conn.Write([]byte(":0\r\n"))
				continue
			}

			if expirySeconds <= 0 {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
				err = saveSnapshot(KeyValueStore, ExpiryStore)
				mu.Unlock()
				if err != nil {
					_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
				} else {
					_, _ = conn.Write([]byte(":1\r\n"))
				}
				continue
			}

			if expirySeconds > int64(math.MaxInt64/int64(time.Second)) {
				mu.Unlock()
				_, _ = conn.Write([]byte("-ERR invalid expiration time\r\n"))
				continue
			}

			ExpiryStore[key] = time.Now().Add(time.Duration(expirySeconds) * time.Second)
			err = saveSnapshot(KeyValueStore, ExpiryStore)
			mu.Unlock()

			if err != nil {
				_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}

		case "PERSIST":
			if len(args) != 2 || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key for 'PERSIST'\r\n"))
				continue
			}

			mu.Lock()
			key := args[1]
			_, exists := KeyValueStore[key]
			if expiry, hasExpiry := ExpiryStore[key]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, key)
				delete(ExpiryStore, key)
				err := saveSnapshot(KeyValueStore, ExpiryStore)
				mu.Unlock()
				if err != nil {
					_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
				} else {
					_, _ = conn.Write([]byte(":0\r\n"))
				}
				continue
			}

			if exists {
				if _, hasExpiry := ExpiryStore[key]; hasExpiry {
					delete(ExpiryStore, key)
					err := saveSnapshot(KeyValueStore, ExpiryStore)
					mu.Unlock()
					if err != nil {
						_, _ = conn.Write([]byte("-ERR failed to save snapshot\r\n"))
					} else {
						_, _ = conn.Write([]byte(":1\r\n"))
					}
					continue
				}
			}
			mu.Unlock()
			_, _ = conn.Write([]byte(":0\r\n"))

		case "TYPE":
			if len(args) != 2 || nullArgs[1] {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments or null key for 'TYPE'\r\n"))
				continue
			}

			mu.Lock()
			_, exists := KeyValueStore[args[1]]
			if expiry, hasExpiry := ExpiryStore[args[1]]; hasExpiry && !time.Now().Before(expiry) {
				delete(KeyValueStore, args[1])
				delete(ExpiryStore, args[1])
				exists = false
			}
			mu.Unlock()

			if exists {
				_, _ = conn.Write([]byte("+string\r\n"))
			} else {
				_, _ = conn.Write([]byte("+none\r\n"))
			}

		default:
			_, _ = conn.Write([]byte("-ERR unknown command\r\n"))
		}

	nextCommand:
	}
}

func main() {
	KeyValueStore, ExpiryStore, err := loadSnapshot()
	if err != nil {
		log.Fatal("Failed to load snapshot:", err)
	}

	mu := &sync.Mutex{}

	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal("Failed to start server:", err)
	}
	defer listener.Close()

	log.Println("Redis is listening on port 6380")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn, KeyValueStore, mu, ExpiryStore)
	}
}
