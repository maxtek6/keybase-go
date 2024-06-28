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
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

const (
	activeCheck string = "expiration"
	uniqueCheck string = "DISTINCT"
	namespace   string = "testnamespace"
	pattern     string = "testpattern"
	key         string = "testkey"
)

var (
	timestamp int64 = time.Now().UnixMilli()
)

func newMock() (*sql.DB, sqlmock.Sqlmock) {
	db, mock, _ := sqlmock.New()
	return db, mock
}

func TestNewTableQuery(t *testing.T) {
	db, mock := newMock()
	tx := newCreateTableQuery()

	mock.ExpectExec(regexp.QuoteMeta(tx.query)).WillReturnError(errors.New("some error"))
	err := tx.queryExec(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectExec(regexp.QuoteMeta(tx.query)).WillReturnResult(sqlmock.NewResult(1, 1))
	err = tx.queryExec(context.TODO(), db)
	assert.NoError(t, err)
}

func TestNewPutQuery(t *testing.T) {
	db, mock := newMock()
	tx := newPutQuery(namespace, key, timestamp)

	mock.ExpectExec(regexp.QuoteMeta(tx.query)).WillReturnError(errors.New("some error"))
	err := tx.queryExec(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectExec(regexp.QuoteMeta(tx.query)).WillReturnResult(sqlmock.NewResult(1, 1))
	err = tx.queryExec(context.TODO(), db)
	assert.NoError(t, err)
}

func TestNewMatchKeyQuery(t *testing.T) {
	tx := newMatchKeyQuery(namespace, pattern, false, false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newMatchKeyQuery(namespace, pattern, false, true, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)

	tx = newMatchKeyQuery(namespace, pattern, true, false, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newMatchKeyQuery(namespace, pattern, true, true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)
}

func TestNewCountKeyQuery(t *testing.T) {
	tx := newCountKeyQuery(namespace, key, false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)

	tx = newCountKeyQuery(namespace, key, true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
}

func TestNewGetKeysQuery(t *testing.T) {
	tx := newGetKeysQuery(namespace, false, false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newGetKeysQuery(namespace, false, true, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)

	tx = newGetKeysQuery(namespace, true, false, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newGetKeysQuery(namespace, true, true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)
}

func TestNewCountKeysQuery(t *testing.T) {
	tx := newCountKeysQuery(namespace, false, false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newCountKeysQuery(namespace, false, true, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)

	tx = newCountKeysQuery(namespace, true, false, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newCountKeysQuery(namespace, true, true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)
}

func TestGetNamespacesQuery(t *testing.T) {
	tx := newGetNamespacesQuery(false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)

	tx = newGetNamespacesQuery(true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
}

func TestCountNamespacesQuery(t *testing.T) {
	tx := newCountNamespacesQuery(false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)

	tx = newCountNamespacesQuery(true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
}

func TestNewCountEntriesQuery(t *testing.T) {
	tx := newCountEntriesQuery(false, false, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newCountEntriesQuery(false, true, timestamp)
	assert.NotContains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)

	tx = newCountEntriesQuery(true, false, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.NotContains(t, tx.query, uniqueCheck)

	tx = newCountEntriesQuery(true, true, timestamp)
	assert.Contains(t, tx.query, activeCheck)
	assert.Contains(t, tx.query, uniqueCheck)
}

func TestNewPruneEntriesQuery(t *testing.T) {
	db, mock := newMock()
	tx := newPruneEntriesQuery(timestamp)

	mock.ExpectExec(regexp.QuoteMeta(tx.query)).WillReturnError(errors.New("some error"))
	err := tx.queryExec(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectExec(regexp.QuoteMeta(tx.query)).WillReturnResult(sqlmock.NewResult(1, 1))
	err = tx.queryExec(context.TODO(), db)
	assert.NoError(t, err)
}

func TestQueryCount(t *testing.T) {
	db, mock := newMock()
	tx := &dbtx{query: ""}

	mock.ExpectQuery(tx.query).WillReturnError(errors.New("some error"))
	_, err := tx.queryCount(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectQuery(tx.query).WillReturnRows(sqlmock.NewRows([]string{"col0"}).AddRow("col"))
	_, err = tx.queryCount(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectQuery(tx.query).WillReturnRows(sqlmock.NewRows([]string{"col0"}).AddRow(1))
	_, err = tx.queryCount(context.TODO(), db)
	assert.NoError(t, err)
}

func TestQueryValues(t *testing.T) {
	db, mock := newMock()
	tx := &dbtx{query: ""}

	mock.ExpectQuery(tx.query).WillReturnError(errors.New("some error"))
	_, err := tx.queryValues(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectQuery(tx.query).WillReturnRows(sqlmock.NewRows([]string{"col0", "col1"}).AddRow("col0", "col1"))
	_, err = tx.queryValues(context.TODO(), db)
	assert.Error(t, err)

	mock.ExpectQuery(tx.query).WillReturnRows(sqlmock.NewRows([]string{"col0"}).AddRow("value"))
	_, err = tx.queryValues(context.TODO(), db)
	assert.NoError(t, err)
}
