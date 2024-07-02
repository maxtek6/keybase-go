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
	"strings"

	"github.com/huandu/go-sqlbuilder"
)

type dbtx struct {
	query string
	args  []any
}

func newCreateTableQuery() *dbtx {
	return &dbtx{
		query: `CREATE TABLE IF NOT EXISTS keybase(namespace TEXT, key TEXT, expiration INTEGER);
		 CREATE INDEX IF NOT EXISTS namespace_index ON keybase(namespace);
		 CREATE INDEX IF NOT EXISTS key_index ON keybase(key);`,
	}
}

func newPutQuery(namespace, key string, expiration int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewInsertBuilder()
	tx.query, tx.args = builder.InsertInto("keybase").Cols("namespace", "key", "expiration").Values(namespace, key, expiration).Build()
	return tx
}

func newMatchKeyQuery(namespace, pattern string, active, unique bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder()
	if unique {
		_ = builder.Distinct()
	}
	_ = builder.Select("key").From("keybase")
	constraints := []string{
		builder.Equal("namespace", namespace),
		builder.Like("key", strings.ReplaceAll(strings.ReplaceAll(pattern, "*", "%"), "?", "_"))}
	if active {
		constraints = append(constraints, builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Where(constraints...).Build()
	return tx
}

func newCountKeyQuery(namespace, key string, active bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder()
	_ = builder.Select("COUNT(key)").From("keybase")
	constraints := []string{
		builder.Equal("namespace", namespace),
		builder.Equal("key", key)}
	if active {
		constraints = append(constraints, builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Where(constraints...).Build()
	return tx
}

func newGetKeysQuery(namespace string, active, unique bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder()
	if unique {
		_ = builder.Distinct()
	}
	_ = builder.Select("key").From("keybase")
	constraints := []string{
		builder.Equal("namespace", namespace)}
	if active {
		constraints = append(constraints, builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Where(constraints...).Build()
	return tx
}

func newCountKeysQuery(namespace string, active, unique bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder()
	col := "COUNT(key)"
	if unique {
		col = "COUNT(DISTINCT key)"
	}
	_ = builder.Select(col).From("keybase")
	constraints := []string{
		builder.Equal("namespace", namespace)}
	if active {
		constraints = append(constraints, builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Where(constraints...).Build()
	return tx
}

func newGetNamespacesQuery(active bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder().Distinct()
	_ = builder.Select("namespace").From("keybase")
	if active {
		_ = builder.Where(builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Build()
	return tx
}

func newCountNamespacesQuery(active bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder().Select("COUNT(DISTINCT namespace)").From("keybase")
	if active {
		_ = builder.Where(builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Build()
	return tx
}

func newCountEntriesQuery(active, unique bool, timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewSelectBuilder()
	col := "COUNT(CONCAT(namespace, key))"
	if unique {
		col = "COUNT(DISTINCT CONCAT(namespace, key))"
	}
	_ = builder.Select(col).From("keybase")
	if active {
		_ = builder.Where(builder.GreaterThan("expiration", timestamp))
	}
	tx.query, tx.args = builder.Build()
	return tx
}

func newPruneEntriesQuery(timestamp int64) *dbtx {
	tx := new(dbtx)
	builder := sqlbuilder.NewDeleteBuilder().DeleteFrom("keybase")
	tx.query, tx.args = builder.Where(builder.LessEqualThan("expiration", timestamp)).Build()
	return tx
}

func newClearEntriesQuery() *dbtx {
	return &dbtx{
		query: "DELETE FROM keybase;",
	}
}

func (tx dbtx) queryExec(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, tx.query, tx.args...)
	if err != nil {
		return err
	}
	return nil
}

func (tx dbtx) queryCount(ctx context.Context, db *sql.DB) (int, error) {
	count := 0
	row, err := db.QueryContext(ctx, tx.query, tx.args...)
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

func (tx dbtx) queryValues(ctx context.Context, db *sql.DB) ([]string, error) {
	value := ""
	values := []string{}
	rows, err := db.QueryContext(ctx, tx.query, tx.args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
