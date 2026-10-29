package main

import (
	"encoding/json"
	"os"
	"time"
)

type snapshot struct {
	KeyValueStore map[string]string    `json:"key_value_store"`
	ExpiryStore   map[string]time.Time `json:"expiry_store"`
}

// lastSavedSnapshot keeps the most recent snapshot known to have been saved.
// Commands call saveSnapshot while holding the store mutex, so this state is
// updated and used under that same lock.
var lastSavedSnapshot snapshot

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	cloned := make(map[K]V, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func rememberSnapshot(KeyValueStore map[string]string, ExpiryStore map[string]time.Time) {
	lastSavedSnapshot = snapshot{
		KeyValueStore: cloneMap(KeyValueStore),
		ExpiryStore:   cloneMap(ExpiryStore),
	}
}

func restoreSnapshot(KeyValueStore map[string]string, ExpiryStore map[string]time.Time) {
	clear(KeyValueStore)
	for key, value := range lastSavedSnapshot.KeyValueStore {
		KeyValueStore[key] = value
	}
	clear(ExpiryStore)
	for key, expiry := range lastSavedSnapshot.ExpiryStore {
		ExpiryStore[key] = expiry
	}
}

func saveSnapshot(KeyValueStore map[string]string, ExpiryStore map[string]time.Time) error {
	data, err := json.Marshal(snapshot{
		KeyValueStore: KeyValueStore,
		ExpiryStore:   ExpiryStore,
	})
	if err != nil {
		restoreSnapshot(KeyValueStore, ExpiryStore)
		return err
	}

	tempFile := "dump.json.tmp"
	if err = os.WriteFile(tempFile, data, 0644); err != nil {
		restoreSnapshot(KeyValueStore, ExpiryStore)
		return err
	}

	if err = os.Rename(tempFile, "dump.json"); err != nil {
		_ = os.Remove(tempFile)
		restoreSnapshot(KeyValueStore, ExpiryStore)
		return err
	}

	rememberSnapshot(KeyValueStore, ExpiryStore)
	return nil
}

func loadSnapshot() (map[string]string, map[string]time.Time, error) {
	data, err := os.ReadFile("dump.json")
	if err != nil {
		if os.IsNotExist(err) {
			KeyValueStore := make(map[string]string)
			ExpiryStore := make(map[string]time.Time)
			rememberSnapshot(KeyValueStore, ExpiryStore)
			return KeyValueStore, ExpiryStore, nil
		}
		return nil, nil, err
	}

	loadedSnapshot := snapshot{}
	if err = json.Unmarshal(data, &loadedSnapshot); err != nil {
		return nil, nil, err
	}

	// Support the older snapshot format, which stored the key-value map directly.
	if loadedSnapshot.KeyValueStore == nil {
		legacyStore := make(map[string]string)
		if err = json.Unmarshal(data, &legacyStore); err != nil {
			return nil, nil, err
		}
		loadedSnapshot.KeyValueStore = legacyStore
	}
	if loadedSnapshot.ExpiryStore == nil {
		loadedSnapshot.ExpiryStore = make(map[string]time.Time)
	}

	// Remove keys whose saved expiration time has already passed.
	now := time.Now()
	for key, expiryTime := range loadedSnapshot.ExpiryStore {
		if !now.Before(expiryTime) {
			delete(loadedSnapshot.ExpiryStore, key)
			delete(loadedSnapshot.KeyValueStore, key)
		}
	}

	rememberSnapshot(loadedSnapshot.KeyValueStore, loadedSnapshot.ExpiryStore)
	return loadedSnapshot.KeyValueStore, loadedSnapshot.ExpiryStore, nil
}
