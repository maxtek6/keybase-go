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
	"fmt"
	"os"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestOpenClose(t *testing.T) {
	keybase, err := Open()
	assert.NotNil(t, keybase)
	assert.NoError(t, err)
	defer keybase.Close()
}

func TestPut(t *testing.T) {
	keybase, err := Open()
	assert.NoError(t, err)
	defer keybase.Close()

	err = keybase.Put(context.TODO(), "namespace", "keyvalue")
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(0))
	defer cancel()
	err = keybase.Put(ctx, "namespace", "keyvalue")
	assert.Error(t, err)
}

// TestKey tests MatchKey and CountKey
func TestKey(t *testing.T) {
	namespace := "default"
	keys := []string{
		"key0", "key0", "key1",
	}
	pattern := "key*"
	keybase, err := Open()
	assert.NoError(t, err)
	defer keybase.Close()

	for _, key := range keys {
		err = keybase.Put(context.TODO(), namespace, key)
		assert.NoError(t, err)
	}

	err = keybase.Put(context.TODO(), "othernamespace", "key0")
	assert.NoError(t, err)

	matchedKeys, err := keybase.MatchKey(context.TODO(), namespace, pattern, false)
	assert.Len(t, matchedKeys, 3)
	assert.NoError(t, err)

	matchedKeys, err = keybase.MatchKey(context.TODO(), namespace, pattern, true)
	assert.Len(t, matchedKeys, 2)
	assert.NoError(t, err)

	count, err := keybase.CountKey(context.TODO(), namespace, keys[0])
	assert.Equal(t, 2, count)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(0))
	defer cancel()
	_, err = keybase.MatchKey(ctx, namespace, pattern, false)
	assert.Error(t, err)
	_, err = keybase.CountKey(ctx, namespace, keys[0])
	assert.Error(t, err)
}

// TestKeys tests GetKeys and CountKeys
func TestKeys(t *testing.T) {
	namespace := "default"
	keys := []string{
		"key0", "key0", "key1",
	}
	keybase, err := Open()
	assert.NoError(t, err)
	defer keybase.Close()

	for _, key := range keys {
		err = keybase.Put(context.TODO(), namespace, key)
		assert.NoError(t, err)
	}

	err = keybase.Put(context.TODO(), "othernamespace", "key0")
	assert.NoError(t, err)

	namespaceKeys, err := keybase.GetKeys(context.TODO(), namespace, false)
	assert.Len(t, namespaceKeys, 3)
	assert.NoError(t, err)

	namespaceKeys, err = keybase.GetKeys(context.TODO(), namespace, true)
	assert.Len(t, namespaceKeys, 2)
	assert.NoError(t, err)

	count, err := keybase.CountKeys(context.TODO(), namespace, false)
	assert.Equal(t, 3, count)
	assert.NoError(t, err)

	count, err = keybase.CountKeys(context.TODO(), namespace, true)
	assert.Equal(t, 2, count)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(0))
	defer cancel()
	_, err = keybase.GetKeys(ctx, namespace, false)
	assert.Error(t, err)
	_, err = keybase.CountKeys(ctx, namespace, false)
	assert.Error(t, err)
}

func TestNamespaces(t *testing.T) {
	keybase, err := Open()
	assert.NoError(t, err)
	defer keybase.Close()

	for namespaceIndex := 0; namespaceIndex < 3; namespaceIndex++ {
		namespace := fmt.Sprintf("namespace%d", namespaceIndex)
		err = keybase.Put(context.TODO(), namespace, "key0")
		assert.NoError(t, err)
		err = keybase.Put(context.TODO(), namespace, "key0")
		assert.NoError(t, err)
		err = keybase.Put(context.TODO(), namespace, "key1")
		assert.NoError(t, err)
	}

	namespaces, err := keybase.GetNamespaces(context.TODO())
	assert.Len(t, namespaces, 3)
	assert.NoError(t, err)

	count, err := keybase.CountNamespaces(context.TODO())
	assert.Equal(t, 3, count)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(0))
	defer cancel()
	_, err = keybase.GetNamespaces(ctx)
	assert.Error(t, err)
	_, err = keybase.CountNamespaces(ctx)
	assert.Error(t, err)
}

// TestEntries tests CountEntries and PruneEntries
func TestEntries(t *testing.T) {
	countStaleEntries := "SELECT COUNT(*) FROM keybase WHERE expiration <= (?);"
	keybase, err := Open(WithTTL(time.Millisecond * 50))
	assert.NoError(t, err)
	defer keybase.Close()

	for namespaceIndex := 0; namespaceIndex < 3; namespaceIndex++ {
		namespace := fmt.Sprintf("namespace%d", namespaceIndex)
		err = keybase.Put(context.TODO(), namespace, "key0")
		assert.NoError(t, err)
		err = keybase.Put(context.TODO(), namespace, "key0")
		assert.NoError(t, err)
		err = keybase.Put(context.TODO(), namespace, "key1")
		assert.NoError(t, err)
	}

	count, err := keybase.CountEntries(context.TODO(), false)
	assert.Equal(t, 9, count)
	assert.NoError(t, err)

	count, err = keybase.CountEntries(context.TODO(), true)
	assert.Equal(t, 6, count)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 50)

	count, err = queryCount(context.TODO(), keybase.db, countStaleEntries, time.Now().UnixMilli())
	assert.Equal(t, 9, count)
	assert.NoError(t, err)

	err = keybase.PruneEntries(context.TODO())
	assert.NoError(t, err)

	count, err = queryCount(context.TODO(), keybase.db, countStaleEntries, time.Now().UnixMilli())
	assert.Zero(t, count)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(0))
	defer cancel()
	_, err = keybase.CountEntries(ctx, true)
	assert.Error(t, err)
	err = keybase.PruneEntries(ctx)
	assert.Error(t, err)
}

// TestStorage tests filesystem
func TestStorage(t *testing.T) {
	storageDirectory, _ := os.MkdirTemp(os.TempDir(), "keybase-*")
	storagePath := path.Join(storageDirectory, "keybase.db")
	initAndStore := func(ctx context.Context) {
		keybase, err := Open(WithStorage(storagePath))
		assert.NoError(t, err)
		assert.NotNil(t, keybase)
		defer keybase.Close()
		for namespace := 0; namespace < 3; namespace++ {
			for key := 0; key < 3; key++ {
				err = keybase.Put(ctx, fmt.Sprintf("namespace%d", namespace), fmt.Sprintf("key%d", key))
				assert.NoError(t, err)
			}
		}
	}
	loadAndCount := func(ctx context.Context) int {
		keybase, err := Open(WithStorage(storagePath))
		assert.NoError(t, err)
		assert.NotNil(t, keybase)
		defer keybase.Close()
		count, err := keybase.CountEntries(ctx, true)
		assert.NoError(t, err)
		return count
	}

	_, err := Open(WithStorage(storageDirectory))
	assert.Error(t, err)

	initAndStore(context.TODO())
	count := loadAndCount(context.TODO())
	assert.Equal(t, 9, count)
}

// TestQuery covers remaining errors in queryKeys and queryCount helper functions
func TestQuery(t *testing.T) {
	db, mock, _ := sqlmock.New()
	timestamp := time.Now().UnixMilli()

	mock.ExpectQuery(regexp.QuoteMeta(getNamespacesQuery)).WithArgs(timestamp).WillReturnRows(sqlmock.NewRows([]string{"col0", "col1"}).AddRow(1, 1))
	_, err := queryKeys(context.TODO(), db, getNamespacesQuery, timestamp)
	assert.Error(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(countNamespacesQuery)).WithArgs(timestamp).WillReturnRows(sqlmock.NewRows([]string{"col0"}).AddRow("string0"))
	_, err = queryCount(context.TODO(), db, countNamespacesQuery, timestamp)
	assert.Error(t, err)
}
