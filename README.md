# Keybase

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://pkg.go.dev/github.com/maxtek6/keybase-go)
[![codecov](https://codecov.io/gh/maxtek6/keybase-go/branch/master/graph/badge.svg)](https://codecov.io/gh/maxtek6/keybase-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxtek6/keybase-go)](https://goreportcard.com/report/github.com/maxtek6/keybase-go)

Keybase is a key counting database with expiring keys and optional persistence.

## Usage

Keybase is designed to work out of the box with minimal configuration. To use keybase,
call the `Open()` function:

```go
kb, err := keybase.Open(keybase.WithStorage("/tmp/keybase.db"), keybase.WithTTL(time.Minute))
```

This will initialize a database at `/tmp/keybase.db` with a key timeout of one minute. Once
keybase is open, it is ready to store and maintain keys. Each key is assigned to a nanespace
and can be inserted using the `Put` function:

```go
_ = kb.Put(context.TODO(), "namespace", "key")
```

Once the key is stored, various functions can be used to query key and namespace information,
such as `GetKeys()`, which will return a slice of strings representing all keys in a given
namespace:

```go
unique := true
keys, err := kb.GetKeys(context.TODO(), "namespace", unique)
```

By setting `unique` to `true`, the slice will only include each active key once. Otherwise,
the string make contain multiple copies of the same key if it has been submitted multiple
times and queried within the TTL duration. Over time, as the keys become stale, they can be
removed using the `PruneEntries` function:

```go
_ = kb.PruneEntries(context.TODO())
```

This will remove the stale keys and reduce the amount of storage required by memory or 
filesystem. When the keybase is no longer needed, it needs to be disconnected using the
`Close()` function:

```go
kb.Close()
```