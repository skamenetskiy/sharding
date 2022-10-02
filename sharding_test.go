package sharding

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

type dummyStrategy[KeyType ID, ConnType any] struct{}

func (dummyStrategy[KeyType, ConnType]) Find(key KeyType, shards []Shard[ConnType]) Shard[ConnType] {
	return &shard[ConnType]{}
}

type dummyHash[KeyType ID] struct{}

func (dummyHash[KeyType]) Sum(_ KeyType) uint64 {
	return 1
}

func TestConnConfig_valid(t *testing.T) {
	type fields struct {
		ID int64

		DSN string
	}
	tests := []struct {
		t       string
		name    string
		fields  fields
		wantErr bool
	}{
		{"int64", "int64", fields{ID: 1, DSN: "dsn"}, false},
		{"int64", "int64 bad dsn", fields{ID: 1, DSN: ""}, true},
		{"int64", "bad int64", fields{ID: 0, DSN: "dsn"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgInt64 := &ShardConfig{
				ID:   tt.fields.ID,
				Addr: tt.fields.DSN,
			}
			if err := cfgInt64.valid(); (err != nil) != tt.wantErr {
				t.Errorf("valid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	type args struct {
		ctx     context.Context
		connect func(_ context.Context, dsn string) (struct{}, error)
		calc    Strategy[uint64, struct{}]
		configs []ShardConfig
	}
	dh := NewDefaultStrategy[uint64, struct{}](nil)
	tests := []struct {
		name    string
		args    args
		want    Cluster[uint64, struct{}]
		wantErr bool
	}{
		{
			"1",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				dh,
				[]ShardConfig{
					{1, "1"},
				},
			},
			&cluster[uint64, struct{}]{
				list: []Shard[struct{}]{
					&shard[struct{}]{1, struct{}{}},
				},
				calc: dh,
			},
			false,
		},
		{
			"2",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				dh,
				[]ShardConfig{},
			},
			nil,
			true,
		},
		{
			"3",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				dh,
				[]ShardConfig{
					{0, ""},
				},
			},
			nil,
			true,
		},
		{
			"4",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, errors.New("error")
				},
				dh,
				[]ShardConfig{
					{1, "1"},
				},
			},
			nil,
			true,
		},
		{
			"5",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				new(dummyStrategy[uint64, struct{}]),
				[]ShardConfig{
					{1, "1"},
				},
			},
			&cluster[uint64, struct{}]{
				list: []Shard[struct{}]{
					&shard[struct{}]{1, struct{}{}},
				},
				calc: new(dummyStrategy[uint64, struct{}]),
			},
			false,
		},
		{
			"6",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				nil,
				[]ShardConfig{
					{1, "1"},
				},
			},
			&cluster[uint64, struct{}]{
				list: []Shard[struct{}]{
					&shard[struct{}]{1, struct{}{}},
				},
				calc: dh,
			},
			false,
		},
		{
			"7",
			args{
				context.Background(),
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				nil,
				[]ShardConfig{
					{2, "2"},
					{3, "3"},
					{1, "1"},
				},
			},
			&cluster[uint64, struct{}]{
				list: []Shard[struct{}]{
					&shard[struct{}]{1, struct{}{}},
					&shard[struct{}]{2, struct{}{}},
					&shard[struct{}]{3, struct{}{}},
				},
				calc: dh,
			},
			false,
		},
		{
			"8",
			args{
				context.Background(),
				nil,
				nil,
				[]ShardConfig{
					{2, "2"},
					{3, "3"},
					{1, "1"},
				},
			},
			nil,
			true,
		},
		{
			"9",
			args{
				nil,
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				nil,
				[]ShardConfig{
					{2, "2"},
					{3, "3"},
					{1, "1"},
				},
			},
			&cluster[uint64, struct{}]{
				list: []Shard[struct{}]{
					&shard[struct{}]{1, struct{}{}},
					&shard[struct{}]{2, struct{}{}},
					&shard[struct{}]{3, struct{}{}},
				},
				calc: dh,
			},
			false,
		},
		{
			"9",
			args{
				nil,
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				nil,
				[]ShardConfig{
					{1, "2"},
					{3, "3"},
					{1, "1"},
				},
			},
			nil,
			true,
		},
		{
			"10",
			args{
				nil,
				func(_ context.Context, _ string) (struct{}, error) {
					return struct{}{}, nil
				},
				nil,
				[]ShardConfig{
					{2, "2"},
					{3, "1"},
					{1, "1"},
				},
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config[uint64, struct{}]{
				Context:  tt.args.ctx,
				Connect:  tt.args.connect,
				Strategy: tt.args.calc,
				Shards:   tt.args.configs,
			}
			got, err := Connect(cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connect() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_AllShards(t *testing.T) {
	type fields struct {
		list []Shard[struct{}]
	}
	tests := []struct {
		name   string
		fields fields
		want   []Shard[struct{}]
	}{
		{
			"1",
			fields{[]Shard[struct{}]{
				&shard[struct{}]{0, struct{}{}},
			}},
			[]Shard[struct{}]{
				&shard[struct{}]{0, struct{}{}},
			},
		},
		{
			"2",
			fields{[]Shard[struct{}]{
				&shard[struct{}]{0, struct{}{}},
				&shard[struct{}]{1, struct{}{}},
			}},
			[]Shard[struct{}]{
				&shard[struct{}]{0, struct{}{}},
				&shard[struct{}]{1, struct{}{}},
			},
		},
		{
			"3",
			fields{[]Shard[struct{}]{
				&shard[struct{}]{0, struct{}{}},
				&shard[struct{}]{1, struct{}{}},
				&shard[struct{}]{2, struct{}{}},
			}},
			[]Shard[struct{}]{
				&shard[struct{}]{0, struct{}{}},
				&shard[struct{}]{1, struct{}{}},
				&shard[struct{}]{2, struct{}{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, struct{}]{
				list: tt.fields.list,
			}
			if got := c.All(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("All() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Shard(t *testing.T) {
	type fields struct {
		list []Shard[struct{}]
		calc Strategy[uint64, struct{}]
	}
	type args struct {
		id uint64
	}
	ff := fields{
		list: []Shard[struct{}]{
			&shard[struct{}]{1, struct{}{}},
			&shard[struct{}]{2, struct{}{}},
			&shard[struct{}]{3, struct{}{}},
		},
		calc: NewDefaultStrategy[uint64, struct{}](nil),
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{"120", ff, args{120}, 1},
		{"121", ff, args{121}, 1},
		{"122", ff, args{122}, 1},
		{"123", ff, args{123}, 1},
		{"124", ff, args{124}, 3},
		{"125", ff, args{125}, 1},
		{"126", ff, args{126}, 3},
		{"127", ff, args{127}, 1},
		{"128", ff, args{128}, 3},
		{"129", ff, args{129}, 2},

		{"1201005000", ff, args{1201005000}, 3},
		{"1211005001", ff, args{1211005001}, 1},
		{"1221005002", ff, args{1221005002}, 1},
		{"1231005003", ff, args{1231005003}, 2},
		{"1241005004", ff, args{1241005004}, 3},
		{"1251005005", ff, args{1251005005}, 2},
		{"1261005006", ff, args{1261005006}, 1},
		{"1271005007", ff, args{1271005007}, 1},
		{"1281005008", ff, args{1281005008}, 3},
		{"1291005009", ff, args{1291005009}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, struct{}]{
				list: tt.fields.list,
				calc: tt.fields.calc,
			}
			if got := c.One(tt.args.id).ID(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("One() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_defaultHash(t *testing.T) {
	type args struct {
		int64Id  int64
		uint64Id uint64
		stringId string
		bytesId  []byte
	}
	tests := []struct {
		t    string
		name string
		args args
		want any
	}{
		{"int64", "in64", args{int64Id: 122}, uint64(4733761633363427328)},
		{"uint64", "uin64", args{uint64Id: 122}, uint64(4733761633363427328)},
		{"string", "string", args{stringId: "122"}, uint64(4733761633363427328)},
		{"int64", "in64", args{int64Id: 123}, uint64(4612164443424423936)},
		{"uint64", "uin64", args{uint64Id: 123}, uint64(4612164443424423936)},
		{"string", "string", args{stringId: "123"}, uint64(4612164443424423936)},
		{"bytes", "[]byte", args{bytesId: []byte("123")}, uint64(4612164443424423936)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.t {
			case "int64":
				if got := NewDefaultHash[int64]().Sum(tt.args.int64Id); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			case "uint64":
				if got := NewDefaultHash[uint64]().Sum(tt.args.uint64Id); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			case "string":
				if got := NewDefaultHash[string]().Sum(tt.args.stringId); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			case "bytes":
				if got := NewDefaultHash[[]byte]().Sum(tt.args.bytesId); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func Test_shard_Conn(t *testing.T) {
	type fields struct {
		id   int64
		conn struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   struct{}
	}{
		{"1", fields{1, struct{}{}}, struct{}{}},
		{"2", fields{2, struct{}{}}, struct{}{}},
		{"3", fields{3, struct{}{}}, struct{}{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard[struct{}]{
				id:   tt.fields.id,
				conn: tt.fields.conn,
			}
			if got := s.Conn(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Conn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_shard_ID(t *testing.T) {
	type fields struct {
		id   int64
		conn struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{"1", fields{1, struct{}{}}, 1},
		{"2", fields{2, struct{}{}}, 2},
		{"3", fields{3, struct{}{}}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard[struct{}]{
				id:   tt.fields.id,
				conn: tt.fields.conn,
			}
			if got := s.ID(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Map(t *testing.T) {
	sh := []Shard[struct{}]{
		&shard[struct{}]{1, struct{}{}},
		&shard[struct{}]{2, struct{}{}},
		&shard[struct{}]{3, struct{}{}},
	}
	dh := NewDefaultStrategy[uint64, struct{}](nil)
	type fields struct {
		list []Shard[struct{}]
		calc Strategy[uint64, struct{}]
	}
	type args struct {
		ids []uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[Shard[struct{}]][]uint64
	}{
		{
			"1",
			fields{sh, dh},
			args{ids: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			map[Shard[struct{}]][]uint64{
				sh[0]: {1, 7, 10},
				sh[1]: {4, 9},
				sh[2]: {2, 3, 5, 6, 8},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, struct{}]{
				list: tt.fields.list,
				calc: tt.fields.calc,
			}
			if got := c.Map(tt.args.ids); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Map() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Each(t *testing.T) {
	sh := []Shard[struct{}]{
		&shard[struct{}]{1, struct{}{}},
		&shard[struct{}]{2, struct{}{}},
		&shard[struct{}]{3, struct{}{}},
	}
	dh := NewDefaultStrategy[uint64, struct{}](nil)
	type fields struct {
		list []Shard[struct{}]
		calc Strategy[uint64, struct{}]
	}
	type args struct {
		fn func(s Shard[struct{}]) error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"1",
			fields{sh, dh},
			args{func(s Shard[struct{}]) error {
				return nil
			}},
			false,
		},
		{
			"1",
			fields{sh, dh},
			args{func(s Shard[struct{}]) error {
				return errors.New("error")
			}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, struct{}]{
				list: tt.fields.list,
				calc: tt.fields.calc,
			}
			if err := c.Each(tt.args.fn); (err != nil) != tt.wantErr {
				t.Errorf("Each() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_cluster_ByKey(t *testing.T) {
	sh := []Shard[struct{}]{
		&shard[struct{}]{1, struct{}{}},
		&shard[struct{}]{2, struct{}{}},
		&shard[struct{}]{3, struct{}{}},
	}
	dh := NewDefaultStrategy[uint64, struct{}](nil)
	type fields struct {
		list []Shard[struct{}]
		calc Strategy[uint64, struct{}]
	}
	type args struct {
		ids []uint64
		fn  func([]uint64, Shard[struct{}]) error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"1",
			fields{sh, dh},
			args{
				[]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				func(ids []uint64, s Shard[struct{}]) error {
					return nil
				},
			},
			false,
		},
		{
			"2",
			fields{sh, dh},
			args{
				[]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				func(ids []uint64, s Shard[struct{}]) error {
					if len(ids)%2 == 0 {
						return errors.New("error")
					}
					return nil
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, struct{}]{
				list: tt.fields.list,
				calc: tt.fields.calc,
			}
			if err := c.ByKeys(tt.args.ids, tt.args.fn); (err != nil) != tt.wantErr {
				t.Errorf("ByKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestShardsConfigFromEnv(t *testing.T) {
	type args struct {
		prefix []string
	}
	type envVar struct {
		key, value string
	}
	tests := []struct {
		name string
		args args
		envs []envVar
		want []ShardConfig
	}{
		{
			"empty",
			args{},
			[]envVar{},
			[]ShardConfig{},
		},
		{
			"one",
			args{},
			[]envVar{
				{"SHARD_ADDRESS_1", "1"},
			},
			[]ShardConfig{
				{1, "1"},
			},
		},
		{
			"one with prefix 1",
			args{[]string{"TEST"}},
			[]envVar{
				{"TEST_SHARD_ADDRESS_1", "1"},
			},
			[]ShardConfig{
				{1, "1"},
			},
		},
		{
			"one with prefix",
			args{[]string{"TEST_"}},
			[]envVar{
				{"TEST_SHARD_ADDRESS_1", "1"},
			},
			[]ShardConfig{
				{1, "1"},
			},
		},
		{
			"three",
			args{},
			[]envVar{
				{"SHARD_ADDRESS_1", "1"},
				{"SHARD_ADDRESS_2", "2"},
				{"SHARD_ADDRESS_3", "3"},
			},
			[]ShardConfig{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
		},
		{
			"three with prefix 1",
			args{[]string{"TEST"}},
			[]envVar{
				{"TEST_SHARD_ADDRESS_1", "1"},
				{"TEST_SHARD_ADDRESS_2", "2"},
				{"TEST_SHARD_ADDRESS_3", "3"},
			},
			[]ShardConfig{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
		},
		{
			"three with prefix",
			args{[]string{"TEST_"}},
			[]envVar{
				{"TEST_SHARD_ADDRESS_1", "1"},
				{"TEST_SHARD_ADDRESS_2", "2"},
				{"TEST_SHARD_ADDRESS_3", "3"},
			},
			[]ShardConfig{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
		},
		{
			"fallback",
			args{},
			[]envVar{
				{"SHARD_ADDRESS", "1"},
			},
			[]ShardConfig{
				{1, "1"},
			},
		},
		{
			"fallback with prefix 1",
			args{[]string{"TEST"}},
			[]envVar{
				{"TEST_SHARD_ADDRESS", "1"},
			},
			[]ShardConfig{
				{1, "1"},
			},
		},
		{
			"fallback with prefix 2",
			args{[]string{"TEST_"}},
			[]envVar{
				{"TEST_SHARD_ADDRESS", "1"},
			},
			[]ShardConfig{
				{1, "1"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			for _, e := range tt.envs {
				_ = os.Setenv(e.key, e.value)
			}
			if got := ShardsConfigFromEnv(tt.args.prefix...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ShardsConfigFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_areShardsUnique(t *testing.T) {
	type args struct {
		shards []ShardConfig
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"unique one",
			args{[]ShardConfig{
				{1, "1"},
			}},
			true,
		},
		{
			"unique three",
			args{[]ShardConfig{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			}},
			true,
		},
		{
			"non unique id",
			args{[]ShardConfig{
				{1, "1"},
				{1, "2"},
				{1, "3"},
			}},
			false,
		},
		{
			"non unique id",
			args{[]ShardConfig{
				{1, "1"},
				{2, "1"},
				{3, "1"},
			}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := areShardsUnique(tt.args.shards); got != tt.want {
				t.Errorf("areShardsUnique() = %v, want %v", got, tt.want)
			}
		})
	}
}
