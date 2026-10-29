
package main

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"
)

func setupTest(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()

	client, server := net.Pipe()

	KeyValueStore := make(map[string]string)
	ExpiryStore := make(map[string]time.Time)
	var mu sync.Mutex

	go handleConnection(server, KeyValueStore, &mu, ExpiryStore)

	t.Cleanup(func() {
		client.Close()
		server.Close()
	})

	return client, bufio.NewReader(client)
}

func sendCommand(t *testing.T, client net.Conn, command string) {
	t.Helper()

	_, err := client.Write([]byte(command))
	if err != nil {
		t.Fatal(err)
	}
}

func readResponse(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}

	return response
}

func TestPing(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*1\r\n$4\r\nPING\r\n")

	response := readResponse(t, reader)

	if response != "+PONG\r\n" {
		t.Fatalf("expected %q, got %q", "+PONG\r\n", response)
	}
}

func TestSetGet(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*3\r\n$3\r\nSET\r\n$5\r\nfruit\r\n$5\r\napple\r\n")

	response := readResponse(t, reader)
	if response != "+OK\r\n" {
		t.Fatalf("SET: expected %q, got %q", "+OK\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$3\r\nGET\r\n$5\r\nfruit\r\n")

	lengthResponse := readResponse(t, reader)
	if lengthResponse != "$5\r\n" {
		t.Fatalf("GET length: expected %q, got %q", "$5\r\n", lengthResponse)
	}

	valueResponse := readResponse(t, reader)
	if valueResponse != "apple\r\n" {
		t.Fatalf("GET value: expected %q, got %q", "apple\r\n", valueResponse)
	}
}

func TestGetMissingKey(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n")

	response := readResponse(t, reader)

	if response != "$-1\r\n" {
		t.Fatalf("expected %q, got %q", "$-1\r\n", response)
	}
}

func TestDeleteKey(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	if response := readResponse(t, reader); response != "+OK\r\n" {
		t.Fatalf("SET: expected %q, got %q", "+OK\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
	if response := readResponse(t, reader); response != ":1\r\n" {
		t.Fatalf("DEL: expected %q, got %q", ":1\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	if response := readResponse(t, reader); response != "$-1\r\n" {
		t.Fatalf("GET after DEL: expected %q, got %q", "$-1\r\n", response)
	}
}

func TestExistsKey(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	if response := readResponse(t, reader); response != "+OK\r\n" {
		t.Fatalf("SET: expected %q, got %q", "+OK\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n")
	if response := readResponse(t, reader); response != ":1\r\n" {
		t.Fatalf("EXISTS existing key: expected %q, got %q", ":1\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$6\r\nEXISTS\r\n$7\r\nmissing\r\n")
	if response := readResponse(t, reader); response != ":0\r\n" {
		t.Fatalf("EXISTS missing key: expected %q, got %q", ":0\r\n", response)
	}
}

func TestIncr(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$1\r\n5\r\n")
	if response := readResponse(t, reader); response != "+OK\r\n" {
		t.Fatalf("SET: expected %q, got %q", "+OK\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n")
	if response := readResponse(t, reader); response != ":6\r\n" {
		t.Fatalf("INCR: expected %q, got %q", ":6\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n")

	if response := readResponse(t, reader); response != "$1\r\n" {
		t.Fatalf("GET length: expected %q, got %q", "$1\r\n", response)
	}

	if response := readResponse(t, reader); response != "6\r\n" {
		t.Fatalf("GET value: expected %q, got %q", "6\r\n", response)
	}
}

func TestAppend(t *testing.T) {
	client, reader := setupTest(t)

	sendCommand(t, client, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nhel\r\n")
	if response := readResponse(t, reader); response != "+OK\r\n" {
		t.Fatalf("SET: expected %q, got %q", "+OK\r\n", response)
	}

	sendCommand(t, client, "*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$2\r\nlo\r\n")
	if response := readResponse(t, reader); response != ":5\r\n" {
		t.Fatalf("APPEND: expected %q, got %q", ":5\r\n", response)
	}

	sendCommand(t, client, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")

	if response := readResponse(t, reader); response != "$5\r\n" {
		t.Fatalf("GET length: expected %q, got %q", "$5\r\n", response)
	}

	if response := readResponse(t, reader); response != "hello\r\n" {
		t.Fatalf("GET value: expected %q, got %q", "hello\r\n", response)
	}
}
