package sharding

import (
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Connect to database using configs.
func Connect[KeyType ID, ConnType any](cfg Config[KeyType, ConnType]) (Cluster[KeyType, ConnType], error) {
	if len(cfg.Shards) == 0 {
		return nil, errors.New("at least one shard config is required")
	}
	if !areShardsUnique(cfg.Shards) {
		return nil, errors.New("shard configurations are not unique")
	}
	if cfg.Connect == nil {
		return nil, errors.New("connect func cannot be nil")
	}
	var (
		c = &cluster[KeyType, ConnType]{
			list: make([]Shard[ConnType], 0, len(cfg.Shards)),
		}
		errCh = make(chan error, len(cfg.Shards))
		ctx   = cfg.Context
		mu    sync.Mutex
		wg    sync.WaitGroup
		err   error
	)
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Strategy != nil {
		c.calc = cfg.Strategy
	} else {
		c.calc = NewDefaultStrategy[KeyType, ConnType](nil)
	}
	for _, sc := range cfg.Shards {
		if err = sc.valid(); err != nil {
			return nil, err
		}
		wg.Add(1)
		go func(id int64, dsn string) {
			defer wg.Done()
			conn, err := cfg.Connect(ctx, dsn)
			if err != nil {
				errCh <- err
				return
			}
			s := &shard[ConnType]{id, conn}
			mu.Lock()
			c.list = append(c.list, s)
			mu.Unlock()
		}(sc.ID, sc.Addr)
	}
	wg.Wait()
	close(errCh)
	if err = <-errCh; err != nil {
		return nil, err
	}
	sort.Slice(c.list, func(i, j int) bool {
		return c.list[i].ID() < c.list[j].ID()
	})
	return c, nil
}

// Config struct.
type Config[KeyType ID, ConnType any] struct {
	Connect  ConnectFunc[ConnType]       // required. connection func
	Shards   []ShardConfig               // required. shards config.
	Context  context.Context             // optional. defaults to context.Background()
	Strategy Strategy[KeyType, ConnType] // optional. defaults to defaultStrategy.
}

// ID type definition.
type ID interface {
	string | []byte | int64 | uint64
}

// ConnectFunc wraps connection func.
type ConnectFunc[ConnType any] func(ctx context.Context, addr string) (ConnType, error)

// ShardConfig type include constant connection id and dsn.
type ShardConfig struct {
	ID   int64  `json:"id"`
	Addr string `json:"dsn"`
}

func (cfg *ShardConfig) valid() error {
	if cfg.ID == 0 {
		return errors.New("validation: invalid shard id")
	}
	if strings.TrimSpace(cfg.Addr) == "" {
		return errors.New("validation: invalid dsn")
	}
	return nil
}

const shardAddr = "SHARD_ADDRESS"

// ShardsConfigFromEnv loads parses environment variables and searches for
// variables called [prefix_]SHARD_ADDRESS_n, where prefix is optional and
// n is an increment number.
func ShardsConfigFromEnv(prefix ...string) []ShardConfig {
	p := ""
	if len(prefix) == 1 {
		if strings.HasSuffix(prefix[0], "_") {
			p = prefix[0]
		} else {
			p = prefix[0] + "_"
		}
	}
	var (
		shards       = make([]ShardConfig, 0)
		id     int64 = 1
	)
	for {
		addr := os.Getenv(fmt.Sprintf("%s%s_%d", p, shardAddr, id))
		if addr == "" {
			break
		}
		shards = append(shards, ShardConfig{id, addr})
		id++
	}
	if len(shards) == 0 {
		if addr := os.Getenv(fmt.Sprintf("%s%s", p, shardAddr)); addr != "" {
			shards = append(shards, ShardConfig{1, addr})
		}
	}
	return shards
}

func areShardsUnique(shards []ShardConfig) bool {
	ids := make(map[int64]struct{}, len(shards))
	addresses := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		if _, ex := ids[s.ID]; ex {
			return false
		}
		ids[s.ID] = struct{}{}
		if _, ex := addresses[s.Addr]; ex {
			return false
		}
		addresses[s.Addr] = struct{}{}
	}
	return true
}

// Cluster interface.
type Cluster[KeyType ID, ConnType any] interface {

	// All returns all shards.
	All() []Shard[ConnType]

	// One returns Shard by key.
	One(key KeyType) Shard[ConnType]

	// Each runs fn on each shard within cluster.
	Each(fn func(s Shard[ConnType]) error) error

	// Map takes a list of identifiers and returns a map[] where the key is the corresponding
	// shard and the value is a slice of ids that belong to shard.
	Map(ids []KeyType) map[Shard[ConnType]][]KeyType

	// ByKeys executes fn on each result of Map func.
	ByKeys(ids []KeyType, fn func([]KeyType, Shard[ConnType]) error) error
}

type cluster[KeyType ID, ConnType any] struct {
	list []Shard[ConnType]
	calc Strategy[KeyType, ConnType]
}

// All returns all shards.
func (c *cluster[KeyType, ConnType]) All() []Shard[ConnType] {
	return c.list
}

// One returns Shard by key.
func (c *cluster[KeyType, ConnType]) One(key KeyType) Shard[ConnType] {
	return c.calc.Find(key, c.list)
}

// Each runs fn on each shard within cluster.
func (c *cluster[KeyType, ConnType]) Each(fn func(s Shard[ConnType]) error) error {
	errCh := make(chan error, len(c.list))
	wg := sync.WaitGroup{}
	for _, s := range c.list {
		wg.Add(1)
		go func(s Shard[ConnType]) {
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
func (c *cluster[KeyType, ConnType]) Map(ids []KeyType) map[Shard[ConnType]][]KeyType {
	res := make(map[Shard[ConnType]][]KeyType, len(ids))
	for _, id := range ids {
		s := c.One(id)
		if _, ok := res[s]; !ok {
			res[s] = make([]KeyType, 0, len(ids))
		}
		res[s] = append(res[s], id)
	}
	return res
}

// ByKeys executes fn on each result of Map func.
func (c *cluster[KeyType, ConnType]) ByKeys(ids []KeyType, fn func([]KeyType, Shard[ConnType]) error) error {
	m := c.Map(ids)
	wg := sync.WaitGroup{}
	errCh := make(chan error, len(m))
	for s, i := range m {
		wg.Add(1)
		go func(ids []KeyType, sh Shard[ConnType]) {
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
type Shard[ConnType any] interface {
	ID() int64
	Conn() ConnType
}

type shard[ConnType any] struct {
	id   int64
	conn ConnType
}

// ID returns ConnIDType.
func (s *shard[ConnType]) ID() int64 {
	return s.id
}

// Conn returns database connection.
func (s *shard[ConnType]) Conn() ConnType {
	return s.conn
}

// Hash interface.
type Hash[KeyType ID] interface {
	Sum(key KeyType) uint64
}

func NewDefaultHash[KeyType ID]() Hash[KeyType] {
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
	case []byte:
		idb = i
	}
	return crc64.Checksum(idb, h.t)
}

// Strategy interface.
type Strategy[KeyType ID, ConnType any] interface {

	// Find shard by key.
	Find(key KeyType, shards []Shard[ConnType]) Shard[ConnType]
}

func NewDefaultStrategy[KeyType ID, ConnType any](
	hash Hash[KeyType],
) Strategy[KeyType, ConnType] {
	if hash == nil {
		hash = NewDefaultHash[KeyType]()
	}
	return &defaultStrategy[KeyType, ConnType]{hash}
}

type defaultStrategy[KeyType ID, ConnType any] struct {
	hash Hash[KeyType]
}

func (c *defaultStrategy[KeyType, ConnType]) Find(
	key KeyType,
	shards []Shard[ConnType],
) Shard[ConnType] {
	return shards[int(c.hash.Sum(key)%uint64(len(shards)))]
}
