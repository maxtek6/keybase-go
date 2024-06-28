// Copyright (c) 2024 Maxtek Consulting
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package keybase

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultTTL     time.Duration = time.Second * 10
	defaultStorage string        = ":memory:"
	invalidCount   int           = -1
)

type options struct {
	storage string
	ttl     time.Duration
}

func parseOptions(opts ...Option) *options {
	config := &options{
		storage: defaultStorage,
		ttl:     defaultTTL,
	}
	for _, opt := range opts {
		switch opt.key {
		case "ttl":
			config.ttl = opt.value.(time.Duration)
		case "storage":
			config.storage = opt.value.(string)
		}
	}
	return config
}

// Set filepath for persistent keybase storage
func WithStorage(path string) Option {
	return Option{
		key:   "storage",
		value: path,
	}
}

// Set TTL for keys
func WithTTL(ttl time.Duration) Option {
	return Option{
		key:   "ttl",
		value: ttl,
	}
}

// Option opaque configuration parameter
type Option struct {
	key   string
	value interface{}
}

// Keybase concurrent key storage with timeouts and optional persistence
type Keybase struct {
	mu  *sync.RWMutex
	db  *sql.DB
	ttl time.Duration
}

// Open opens new or existing keybase
func Open(ctx context.Context, opts ...Option) (*Keybase, error) {
	config := parseOptions(opts...)
	db, err := sqlOpen("sqlite", config.storage)
	if err != nil {
		return nil, fmt.Errorf("keybase.Open: failed to open database: %v", err)
	}
	err = newCreateTableQuery().queryExec(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("keybase.Open: failed to create table: %v", err)
	}
	return &Keybase{
		mu:  new(sync.RWMutex),
		db:  db,
		ttl: config.ttl,
	}, nil
}

// Close closes keybase
func (k *Keybase) Close() {
	_ = k.db.Close() // error is unreachable
}

// Put inserts new value
func (k *Keybase) Put(ctx context.Context, namespace, key string) error {
	expiration := time.Now().Add(k.ttl).UnixMilli()
	k.mu.Lock()
	defer k.mu.Unlock()
	tx := newPutQuery(namespace, key, expiration)
	err := tx.queryExec(ctx, k.db)
	if err != nil {
		return fmt.Errorf("keybase.Put: failed to insert key: %v", err)
	}
	return nil
}

// MatchKey collect list of keys from a given namespace that match a specific pattern
func (k *Keybase) MatchKey(ctx context.Context, namespace, pattern string, active, unique bool) ([]string, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys, err := newMatchKeyQuery(namespace, pattern, active, unique, timestamp).queryValues(ctx, k.db)
	if err != nil {
		return nil, fmt.Errorf("keybase.MatchKey: failed to query database: %v", err)
	}
	return keys, nil
}

// CountKey count active frequency of a specific key from a given namespace
func (k *Keybase) CountKey(ctx context.Context, namespace, key string, active bool) (int, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := newCountKeyQuery(namespace, key, active, timestamp).queryCount(ctx, k.db)
	if err != nil {
		return invalidCount, fmt.Errorf("keybase.CountKey: failed to query database: %v", err)
	}
	return count, nil
}

// GetKeys collects a list of active keys from a given namespace
func (k *Keybase) GetKeys(ctx context.Context, namespace string, active, unique bool) ([]string, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys, err := newGetKeysQuery(namespace, active, unique, timestamp).queryValues(ctx, k.db)
	if err != nil {
		return nil, fmt.Errorf("keybase.GetKeys: failed to query database: %v", err)
	}
	return keys, nil
}

// CountKeys counts the active keys from a given namespace
func (k *Keybase) CountKeys(ctx context.Context, namespace string, active, unique bool) (int, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := newCountKeysQuery(namespace, active, unique, timestamp).queryCount(ctx, k.db)
	if err != nil {
		return invalidCount, fmt.Errorf("keybase.CountKeys: failed to query database: %v", err)
	}
	return count, nil
}

// GetNamespace collects a list of active namespaces
func (k *Keybase) GetNamespaces(ctx context.Context, active bool) ([]string, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys, err := newGetNamespacesQuery(active, timestamp).queryValues(ctx, k.db)
	if err != nil {
		return nil, fmt.Errorf("keybase.GetNamespaces: failed to query database: %v", err)
	}
	return keys, nil
}

// CountNamespaces counts active namespaces
func (k *Keybase) CountNamespaces(ctx context.Context, active bool) (int, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := newCountNamespacesQuery(active, timestamp).queryCount(ctx, k.db)
	if err != nil {
		return invalidCount, fmt.Errorf("keybase.CountNamespaces: failed to query database: %v", err)
	}
	return count, nil
}

// CountEntries counts all keys in all namespaces
func (k *Keybase) CountEntries(ctx context.Context, active, unique bool) (int, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := newCountEntriesQuery(active, unique, timestamp).queryCount(ctx, k.db)
	if err != nil {
		return invalidCount, fmt.Errorf("keybase.CountEntries: failed to query database: %v", err)
	}
	return count, nil
}

// PruneEntries removes stale entries.
func (k *Keybase) PruneEntries(ctx context.Context) error {
	timestamp := time.Now().UnixMilli()
	k.mu.Lock()
	defer k.mu.Unlock()
	err := newPruneEntriesQuery(timestamp).queryExec(ctx, k.db)
	if err != nil {
		return fmt.Errorf("keybase.PruneEntries: failed to insert key: %v", err)
	}
	return nil
}

func sqlOpen(driverName string, dataSourceName string) (*sql.DB, error) {
	db, _ := sql.Open(driverName, dataSourceName)
	err := db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}
