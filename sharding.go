package sharding

import (
	"context"
	"errors"
	"hash/crc64"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Connect to database using configs.
func Connect[KeyType ItemID, ConnID ConnectionID, ConnType any](
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
		errCh = make(chan error, len(configs))
		mu    sync.Mutex
		wg    sync.WaitGroup
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
	sort.Slice(c.list, func(i, j int) bool {
		return c.list[i].ID() < c.list[j].ID()
	})
	return c, nil
}

// ItemID type definition.
type ItemID interface {
	string | []byte | int64 | uint64
}

// ConnectionID type definition.
type ConnectionID interface {
	int64 | uint64 | string
}

// ConnFunc wraps connection func.
type ConnFunc[ConnType any] func(ctx context.Context, addr string) (ConnType, error)

// ConnConfig type include constant connection id and dsn.
type ConnConfig[ConnID ConnectionID] struct {
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
type Cluster[KeyType ItemID, ConnID ConnectionID, ConnType any] interface {

	// All returns all shards.
	All() []Shard[ConnID, ConnType]

	// One returns Shard by key.
	One(key KeyType) Shard[ConnID, ConnType]

	// Each runs fn on each shard within cluster.
	Each(fn func(s Shard[ConnID, ConnType]) error) error

	// Map takes a list of identifiers and returns a map[] where the key is the corresponding
	// shard and the value is a slice of ids that belong to shard.
	Map(ids []KeyType) map[Shard[ConnID, ConnType]][]KeyType

	// ByKey executes fn on each result of Map func.
	ByKey(ids []KeyType, fn func([]KeyType, Shard[ConnID, ConnType]) error) error
}

type cluster[KeyType ItemID, ConnID ConnectionID, ConnType any] struct {
	list []Shard[ConnID, ConnType]
	hash Hash[KeyType]
}

// All returns all shards.
func (c *cluster[KeyType, ConnID, ConnType]) All() []Shard[ConnID, ConnType] {
	return c.list
}

// One returns Shard by key.
func (c *cluster[KeyType, ConnID, ConnType]) One(key KeyType) Shard[ConnID, ConnType] {
	return c.list[int(c.hash.Sum(key)%uint64(len(c.list)))]
}

// Each runs fn on each shard within cluster.
func (c *cluster[KeyType, ConnID, ConnType]) Each(fn func(s Shard[ConnID, ConnType]) error) error {
	errCh := make(chan error, len(c.list))
	wg := sync.WaitGroup{}
	for _, s := range c.list {
		wg.Add(1)
		go func(s Shard[ConnID, ConnType]) {
			defer wg.Done()
			if err := fn(s); err != nil {
				errCh <- err
			}
		}(s)
	}
	wg.Wait()
	close(errCh)
	return <-errCh
}

// Map takes a list of identifiers and returns a map[] where the key is the corresponding
// shard and the value is a slice of ids that belong to shard.
func (c *cluster[KeyType, ConnID, ConnType]) Map(ids []KeyType) map[Shard[ConnID, ConnType]][]KeyType {
	res := make(map[Shard[ConnID, ConnType]][]KeyType, len(ids))
	for _, id := range ids {
		s := c.One(id)
		if _, ok := res[s]; !ok {
			res[s] = make([]KeyType, 0, len(ids))
		}
		res[s] = append(res[s], id)
	}
	return res
}

// ByKey executes fn on each result of Map func.
func (c *cluster[KeyType, ConnID, ConnType]) ByKey(ids []KeyType, fn func([]KeyType, Shard[ConnID, ConnType]) error) error {
	m := c.Map(ids)
	wg := sync.WaitGroup{}
	errCh := make(chan error, len(m))
	for s, i := range m {
		wg.Add(1)
		go func(ids []KeyType, sh Shard[ConnID, ConnType]) {
			defer wg.Done()
			if err := fn(ids, sh); err != nil {
				errCh <- err
			}
		}(i, s)
	}
	wg.Wait()
	close(errCh)
	return <-errCh
}

// Shard interface.
type Shard[ConnID ConnectionID, ConnType any] interface {
	ID() ConnID
	Conn() ConnType
}

type shard[ConnID ConnectionID, ConnType any] struct {
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

// Hash interface.
type Hash[KeyType ItemID] interface {
	Sum(key KeyType) uint64
}

func newDefaultHash[KeyType ItemID]() *defaultHash[KeyType] {
	return &defaultHash[KeyType]{crc64.MakeTable(crc64.ISO)}
}

type defaultHash[KeyType ItemID] struct {
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
	case []byte:
		idb = i
	}
	return crc64.Checksum(idb, h.t)
}
