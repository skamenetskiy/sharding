package sharding

import (
	"context"
	"errors"
	"hash/crc64"
	"strconv"
	"strings"
	"sync"
)

// Connect to database using configs.
func Connect[KeyType ID, ConnID ID, ConnType any](
	ctx context.Context,
	connect ConnFunc[ConnType],
	hash Hash[KeyType],
	configs ...ConnConfig[ConnID],
) (Cluster[KeyType, ConnID, ConnType], error) {
	if len(configs) == 0 {
		return nil, errors.New("at least one connection config is required")
	}
	var (
		c = &cluster[KeyType, ConnID, ConnType]{
			list: make([]Shard[ConnID, ConnType], 0, len(configs)),
		}
		mu    sync.Mutex
		wg    sync.WaitGroup
		errCh = make(chan error, len(configs))
		err   error
	)
	for _, sc := range configs {
		if err = sc.valid(); err != nil {
			return nil, err
		}
		wg.Add(1)
		go func(id ConnID, dsn string) {
			defer wg.Done()
			conn, err := connect(ctx, dsn)
			if err != nil {
				errCh <- err
				return
			}
			s := &shard[ConnID, ConnType]{id, conn}
			mu.Lock()
			c.list = append(c.list, s)
			mu.Unlock()
			errCh <- nil
		}(sc.ID, sc.DSN)
	}
	wg.Wait()
	close(errCh)
	if err = <-errCh; err != nil {
		return nil, err
	}
	if hash != nil {
		c.hash = hash
	} else {
		c.hash = newDefaultHash[KeyType]()
	}
	return c, nil
}

// ID type interface.
type ID interface{ string | int64 | uint64 }

// ConnFunc wraps database connection function.
type ConnFunc[ConnType any] func(ctx context.Context, dsn string) (ConnType, error)

// ConnConfig type include constant connection id and dsn.
type ConnConfig[ConnID ID] struct {
	ID  ConnID `json:"id"`
	DSN string `json:"dsn"`
}

func (cfg *ConnConfig[ConnID]) valid() error {
	switch v := any(cfg.ID).(type) {
	case int64:
		if v == 0 {
			return errors.New("0 is not a valid id")
		}
	case uint64:
		if v == 0 {
			return errors.New("0 is not a valid id")
		}
	case string:
		if strings.TrimSpace(v) == "" {
			return errors.New("id cannot be an empty string")
		}
	}
	if strings.TrimSpace(cfg.DSN) == "" {
		return errors.New("validation: invalid dsn")
	}
	return nil
}

// Cluster interface.
type Cluster[KeyType ID, ConnID ID, ConnType any] interface {

	// AllShards returns all shards.
	AllShards() []Shard[ConnID, ConnType]

	// Shard returns Shard by key.
	Shard(key KeyType) Shard[ConnID, ConnType]
}

type cluster[KeyType ID, ConnID ID, ConnType any] struct {
	list []Shard[ConnID, ConnType]
	hash Hash[KeyType]
}

// AllShards returns all shards.
func (c *cluster[KeyType, ConnID, ConnType]) AllShards() []Shard[ConnID, ConnType] {
	return c.list
}

// Shard returns Shard by key.
func (c *cluster[KeyType, ConnID, ConnType]) Shard(id KeyType) Shard[ConnID, ConnType] {
	return c.list[int(c.hash.Sum(id)%uint64(len(c.list)))]
}

// Shard interface.
type Shard[ConnID ID, ConnType any] interface {
	ID() ConnID
	Conn() ConnType
}

type shard[ConnID ID, ConnType any] struct {
	id   ConnID
	conn ConnType
}

// ID returns ConnID.
func (s *shard[ConnID, ConnType]) ID() ConnID {
	return s.id
}

// Conn returns database connection.
func (s *shard[ConnID, ConnType]) Conn() ConnType {
	return s.conn
}

type Hash[KeyType ID] interface {
	Sum(key KeyType) uint64
}

func newDefaultHash[KeyType ID]() *defaultHash[KeyType] {
	return &defaultHash[KeyType]{crc64.MakeTable(crc64.ISO)}
}

type defaultHash[KeyType ID] struct {
	t *crc64.Table
}

// Sum of id.
func (h *defaultHash[KeyType]) Sum(id KeyType) uint64 {
	var idb []byte
	switch i := any(id).(type) {
	case int64:
		idb = strconv.AppendInt(idb, i, 10)
	case uint64:
		idb = strconv.AppendUint(idb, i, 10)
	case string:
		idb = []byte(i)
	}
	return crc64.Checksum(idb, h.t)
}
