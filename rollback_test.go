package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func encodeRESPCommand(args ...string) string {
	var command strings.Builder
	fmt.Fprintf(&command, "*%d\r\n", len(args))

	for _, arg := range args {
		fmt.Fprintf(&command, "$%d\r\n%s\r\n", len(arg), arg)
	}

	return command.String()
}

func runRESPCommandForTest(t *testing.T, KeyValueStore map[string]string, ExpiryStore map[string]time.Time, mu *sync.Mutex, args ...string) string {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		defer close(done)
		handleConnection(serverConn, KeyValueStore, mu, ExpiryStore)
	}()

	if err := clientConn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		clientConn.Close()
		<-done
		t.Fatal(err)
	}

	if _, err := io.WriteString(clientConn, encodeRESPCommand(args...)); err != nil {
		clientConn.Close()
		<-done
		t.Fatalf("failed to write command %v: %v", args, err)
	}

	response, err := bufio.NewReader(clientConn).ReadString('\n')
	clientConn.Close()
	<-done

	if err != nil {
		t.Fatalf("failed to read response for command %v: %v", args, err)
	}

	return response
}

func TestMutationCommandsRollBackWhenSnapshotSaveFails(t *testing.T) {
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Errorf("failed to restore working directory: %v", err)
		}
	}()

	// A directory at this path forces os.WriteFile in saveSnapshot to fail.
	if err := os.Mkdir(filepath.Join(tempDir, "dump.json.tmp"), 0755); err != nil {
		t.Fatal(err)
	}

	expiryTime := time.Now().Add(time.Hour)

	tests := []struct {
		name   string
		args   []string
		store  map[string]string
		expiry map[string]time.Time
	}{
		{
			name:   "SET",
			args:   []string{"SET", "key", "new"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "DEL",
			args:   []string{"DEL", "key"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "INCR",
			args:   []string{"INCR", "key"},
			store:  map[string]string{"key": "10"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "INCRBY",
			args:   []string{"INCRBY", "key", "5"},
			store:  map[string]string{"key": "10"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "DECRBY",
			args:   []string{"DECRBY", "key", "5"},
			store:  map[string]string{"key": "10"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:  "MSET",
			args:  []string{"MSET", "key", "new", "other", "changed"},
			store: map[string]string{"key": "old", "other": "original"},
			expiry: map[string]time.Time{
				"key":   expiryTime,
				"other": expiryTime,
			},
		},
		{
			name:   "APPEND",
			args:   []string{"APPEND", "key", "new"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "GETSET",
			args:   []string{"GETSET", "key", "new"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "SETNX",
			args:   []string{"SETNX", "newkey", "new"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "GETDEL",
			args:   []string{"GETDEL", "key"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "SETEX",
			args:   []string{"SETEX", "key", "60", "new"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "EXPIRE",
			args:   []string{"EXPIRE", "key", "60"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
		{
			name:   "PERSIST",
			args:   []string{"PERSIST", "key"},
			store:  map[string]string{"key": "old"},
			expiry: map[string]time.Time{"key": expiryTime},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := cloneMap(tt.store)
			expiry := cloneMap(tt.expiry)
			wantStore := cloneMap(tt.store)
			wantExpiry := cloneMap(tt.expiry)

			var mu sync.Mutex

			mu.Lock()
			rememberSnapshot(store, expiry)
			mu.Unlock()

			response := runRESPCommandForTest(t, store, expiry, &mu, tt.args...)
			if response != "-ERR failed to save snapshot\r\n" {
				t.Fatalf("unexpected response: %q", response)
			}

			mu.Lock()
			defer mu.Unlock()

			if !reflect.DeepEqual(store, wantStore) {
				t.Errorf("key-value store was not rolled back: got %#v, want %#v", store, wantStore)
			}

			if !reflect.DeepEqual(expiry, wantExpiry) {
				t.Errorf("expiry store was not rolled back: got %#v, want %#v", expiry, wantExpiry)
			}
		})
	}
}
