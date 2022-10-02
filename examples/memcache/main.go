package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/rs/xid"

	"github.com/skamenetskiy/sharding"
)

func main() {
	// connect func
	connect := sharding.ConnectFunc[*memcache.Client](func(_ context.Context, addr string) (*memcache.Client, error) {
		client := memcache.New(addr)
		return client, nil
	})

	// define shards
	shards := []sharding.ShardConfig{
		{
			ID:   1,
			Addr: "127.0.0.1:11211",
		},
		{
			ID:   2,
			Addr: "127.0.0.1:11212",
		},
		{
			ID:   3,
			Addr: "127.0.0.1:11213",
		},
	}

	// connect to database shards
	cluster, err := sharding.Connect[string, *memcache.Client](
		sharding.Config[string, *memcache.Client]{
			Context: context.Background(),
			Connect: connect,
			Shards:  shards,
		},
	)
	if err != nil {
		log.Fatalf("failed to connect: %s\n", err)
	}

	// insert 10 rows, each to corresponding shard
	ids := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		id := xid.New().String()
		value := fmt.Sprintf("sharding example [index: %d, id: %s]", i, id)
		v := &memcache.Item{
			Key:        id,
			Value:      []byte(value),
			Expiration: 60 * 60 * 24,
		}
		ids = append(ids, id)
		if err = cluster.One(id).Conn().Add(v); err != nil {
			log.Fatalf("failed to insert: %s\n", err)
		}
	}

	type item struct {
		id, value string
	}

	res := make([]item, 0, 10)
	mu := sync.Mutex{}

	// query items from all shards in parallel
	if err = cluster.ByKeys(ids, func(ids []string, s sharding.Shard[*memcache.Client]) error {
		items, err := s.Conn().GetMulti(ids)
		if err != nil {
			return err
		}
		for _, i := range items {
			v := item{i.Key, string(i.Value)}
			mu.Lock()
			res = append(res, v)
			mu.Unlock()
		}
		return nil
	}); err != nil {
		log.Fatalf("failed to select: %s\n", err)
	}

	// print out the result
	log.Println(res)
}
