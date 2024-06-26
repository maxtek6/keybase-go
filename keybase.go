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
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultTTL              time.Duration = time.Second * 10
	defaultStorage          string        = ":memory:"
	createTableQuery        string        = "CREATE TABLE IF NOT EXISTS keybase(namespace TEXT, key TEXT, expiration INTEGER);"
	createNamespaceIndex    string        = "CREATE INDEX IF NOT EXISTS namespace_index ON keybase(namespace);"
	createKeyIndex          string        = "CREATE INDEX IF NOT EXISTS key_index ON keybase(key);"
	putQuery                string        = "INSERT INTO keybase VALUES (?, ?, ?);"
	matchKeyQuery           string        = "SELECT key FROM keybase WHERE namespace = (?) AND key LIKE (?) AND expiration > (?);"
	matchKeyUniqueQuery     string        = "SELECT DISTINCT key FROM keybase WHERE namespace = (?) AND key LIKE (?) AND expiration > (?);"
	countKeyQuery           string        = "SELECT COUNT(*) FROM keybase WHERE namespace = (?) AND key = (?) AND expiration > (?);"
	getKeysQuery            string        = "SELECT key FROM keybase WHERE namespace = (?) AND expiration > (?);"
	getKeysUniqueQuery      string        = "SELECT DISTINCT key FROM keybase WHERE namespace = (?) AND expiration > (?);"
	countKeysQuery          string        = "SELECT COUNT(key) FROM keybase WHERE namespace = (?) AND expiration > (?);"
	countKeysUniqueQuery    string        = "SELECT COUNT(DISTINCT key) FROM keybase WHERE namespace = (?) AND expiration > (?);"
	getNamespacesQuery      string        = "SELECT DISTINCT namespace FROM keybase WHERE expiration > (?);"
	countNamespacesQuery    string        = "SELECT COUNT(DISTINCT namespace) FROM keybase WHERE expiration > (?);"
	countEntriesQuery       string        = "SELECT COUNT(*) FROM keybase WHERE expiration > (?);"
	countEntriesUniqueQuery string        = "SELECT COUNT(DISTINCT CONCAT(namespace, key)) FROM keybase WHERE expiration > (?);"
	pruneEntriesQuery       string        = "DELETE FROM keybase WHERE expiration <= (?);"
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
func Open(opts ...Option) (*Keybase, error) {
	config := parseOptions(opts...)
	db, err := sqlOpen("sqlite", config.storage)
	if err != nil {
		return nil, fmt.Errorf("keybase.Open: failed to open database: %v", err)
	}
	initQuery := createTableQuery + createNamespaceIndex + createKeyIndex
	_, _ = db.Exec(initQuery)
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
	k.mu.Lock()
	defer k.mu.Unlock()
	expiration := time.Now().Add(k.ttl).UnixMilli()
	_, err := k.db.ExecContext(ctx, putQuery, namespace, key, expiration)
	if err != nil {
		return fmt.Errorf("keybase.Put: failed to insert key: %v", err)
	}
	return nil
}

// MatchKey collect list of keys from a given namespace that match a specific pattern
func (k *Keybase) MatchKey(ctx context.Context, namespace, pattern string, unique bool) ([]string, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	query := matchKeyQuery
	if unique {
		query = matchKeyUniqueQuery
	}
	queryPattern := strings.ReplaceAll(pattern, "*", "%")
	matches, err := queryKeys(ctx, k.db, query, namespace, queryPattern, timestamp)
	if err != nil {
		return nil, fmt.Errorf("keybase.MatchKey: failed to query database: %v", err)
	}
	return matches, nil
}

// CountKey count active frequency of a specific key from a given namespace
func (k *Keybase) CountKey(ctx context.Context, namespace, key string) (int, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := queryCount(ctx, k.db, countKeyQuery, namespace, key, timestamp)
	if err != nil {
		return -1, fmt.Errorf("keybase.CountKey: failed to query database: %v", err)
	}
	return count, nil
}

// GetKeys collects a list of active keys from a given namespace
func (k *Keybase) GetKeys(ctx context.Context, namespace string, unique bool) ([]string, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	query := getKeysQuery
	if unique {
		query = getKeysUniqueQuery
	}
	keys, err := queryKeys(ctx, k.db, query, namespace, timestamp)
	if err != nil {
		return nil, fmt.Errorf("keybase.GetKeys: failed to query database: %v", err)
	}
	return keys, nil
}

// CountKeys counts the active keys from a given namespace
func (k *Keybase) CountKeys(ctx context.Context, namespace string, unique bool) (int, error) {
	timestamp := time.Now().UnixMilli()
	query := countKeysQuery
	if unique {
		query = countKeysUniqueQuery
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := queryCount(ctx, k.db, query, namespace, timestamp)
	if err != nil {
		return -1, fmt.Errorf("keybase.CountKeys: failed to query database: %v", err)
	}
	return count, nil
}

// GetNamespace collects a list of active namespaces
func (k *Keybase) GetNamespaces(ctx context.Context) ([]string, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys, err := queryKeys(ctx, k.db, getNamespacesQuery, timestamp)
	if err != nil {
		return nil, fmt.Errorf("keybase.GetNamespaces: failed to query database: %v", err)
	}
	return keys, nil
}

// CountNamespaces counts active namespaces
func (k *Keybase) CountNamespaces(ctx context.Context) (int, error) {
	timestamp := time.Now().UnixMilli()
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := queryCount(ctx, k.db, countNamespacesQuery, timestamp)
	if err != nil {
		return -1, fmt.Errorf("keybase.CountNamespaces: failed to query database: %v", err)
	}
	return count, nil
}

// CountEntries counts all keys in all namespaces
func (k *Keybase) CountEntries(ctx context.Context, unique bool) (int, error) {
	timestamp := time.Now().UnixMilli()
	query := countEntriesQuery
	if unique {
		query = countEntriesUniqueQuery
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	count, err := queryCount(ctx, k.db, query, timestamp)
	if err != nil {
		return -1, fmt.Errorf("keybase.CountEntries: failed to query database: %v", err)
	}
	return count, nil
}

// PruneEntries removes stale entries.
func (k *Keybase) PruneEntries(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	timestamp := time.Now().Add(k.ttl).UnixMilli()
	_, err := k.db.ExecContext(ctx, pruneEntriesQuery, timestamp)
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

func queryCount(ctx context.Context, db *sql.DB, query string, args ...any) (int, error) {
	count := -1
	row, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return count, err
	}
	defer func() {
		_ = row.Close()
	}()
	if row.Next() {
		err = row.Scan(&count)
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

func queryKeys(ctx context.Context, db *sql.DB, query string, args ...any) ([]string, error) {
	key := ""
	keys := []string{}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}
