package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/rs/xid"

	"github.com/skamenetskiy/sharding"
)

// This app provides basic example to connect to multiple database shards,
// create tables on all shards. Insert data to specific shards by key
// and then query the data from all shards.
func main() {
	// connect func
	connect := sharding.ConnFunc[*sql.DB](func(_ context.Context, addr string) (*sql.DB, error) {
		return sql.Open("postgres", addr)
	})

	// define shards
	shards := []sharding.ConnConfig[int64]{
		{
			ID:   1,
			Addr: "postgres://postgres:password@localhost:5432/db1",
		},
		{
			ID:   2,
			Addr: "postgres://postgres:password@localhost:5432/db2",
		},
		{
			ID:   3,
			Addr: "postgres://postgres:password@localhost:5432/db3",
		},
	}

	// connect to database shards
	cluster, err := sharding.Connect[string, int64, *sql.DB](
		context.Background(), // use background context
		connect,              // use connect func, defined above
		nil,                  // use default hash algorithm (crc64)
		shards...,            // shards configuration
	)
	if err != nil {
		log.Fatalf("failed to connect: %s\n", err)
	}

	// create tables
	if err = cluster.Each(func(s sharding.Shard[int64, *sql.DB]) error {
		_, err := s.Conn().Exec(
			"CREATE TABLE IF NOT EXISTS sample (id CHAR(20) NOT NULL PRIMARY KEY, value TEXT)")
		return err
	}); err != nil {
		log.Fatalf("failed to create tables: %s\n", err)
	}

	// insert 10 rows, each to corresponding shard
	ids := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		id := xid.New().String()
		value := fmt.Sprintf("sharding example [index: %d, id: %s]", i, id)
		ids = append(ids, id)
		if _, err = cluster.One(id).Conn().
			Exec("INSERT INTO sample (id, value) VALUES ($1, $2)", id, value); err != nil {
			log.Fatalf("failed to insert: %s\n", err)
		}
	}

	type item struct {
		id, value string
	}

	res := make([]item, 0, 10)
	mu := sync.Mutex{}

	// query rows from all shard in parallel
	if err = cluster.ByKey(ids, func(ids []string, s sharding.Shard[int64, *sql.DB]) error {
		pointers := make([]string, len(ids))
		args := make([]any, len(ids))
		for i := 1; i <= len(ids); i++ {
			pointers[i] = fmt.Sprintf("$%d", i)
			args[i] = ids[i-1]
		}
		query := fmt.Sprintf("SELECT id, value FROM sample WHERE id IN (%s)", strings.Join(pointers, ","))
		rows, err := s.Conn().Query(query, args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			i := item{}
			if err = rows.Scan(&i.id, &i.value); err != nil {
				return err
			}
			mu.Lock()
			res = append(res, i)
			mu.Unlock()
		}
		return nil
	}); err != nil {
		log.Fatalf("failed to select: %s\n", err)
	}

	// print out the result
	log.Println(res)
}
