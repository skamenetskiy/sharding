package sharding

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type dummyHash[KeyType ItemID] struct{}

func (dummyHash[KeyType]) Sum(_ KeyType) uint64 {
	return 1
}

func TestConnConfig_valid(t *testing.T) {
	type fields struct {
		IDInt64  int64
		IDUint64 uint64
		IDString string
		DSN      string
	}
	tests := []struct {
		t       string
		name    string
		fields  fields
		wantErr bool
	}{
		{"int64", "int64", fields{IDInt64: 1, DSN: "dsn"}, false},
		{"int64", "int64 bad dsn", fields{IDInt64: 1, DSN: ""}, true},
		{"uint64", "uint64", fields{IDUint64: 1, DSN: "dsn"}, false},
		{"uint64", "uint64", fields{IDUint64: 1, DSN: ""}, true},
		{"string", "string", fields{IDString: "1", DSN: "dsn"}, false},
		{"string", "string", fields{IDString: "1", DSN: ""}, true},
		{"int64", "bad int64", fields{IDInt64: 0, DSN: "dsn"}, true},
		{"uint64", "bad uint64", fields{IDUint64: 0, DSN: "dsn"}, true},
		{"string", "bad string", fields{IDString: "", DSN: "dsn"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.t {
			case "int64":
				cfgInt64 := &ConnConfig[int64]{
					ID:  tt.fields.IDInt64,
					DSN: tt.fields.DSN,
				}
				if err := cfgInt64.valid(); (err != nil) != tt.wantErr {
					t.Errorf("valid() error = %v, wantErr %v", err, tt.wantErr)
				}
			case "uint64":
				cfgUint64 := &ConnConfig[uint64]{
					ID:  tt.fields.IDUint64,
					DSN: tt.fields.DSN,
				}
				if err := cfgUint64.valid(); (err != nil) != tt.wantErr {
					t.Errorf("valid() error = %v, wantErr %v", err, tt.wantErr)
				}
			case "string":
				cfgString := &ConnConfig[string]{
					ID:  tt.fields.IDString,
					DSN: tt.fields.DSN,
				}
				if err := cfgString.valid(); (err != nil) != tt.wantErr {
					t.Errorf("valid() error = %v, wantErr %v", err, tt.wantErr)
				}
			default:
				t.Error("invalid id type")
			}
		})
	}
}

func TestConnect(t *testing.T) {
	type args struct {
		ctx     context.Context
		connect func(_ context.Context, dsn string) (struct{}, error)
		hash    Hash[uint64]
		configs []ConnConfig[uint64]
	}
	dh := newDefaultHash[uint64]()
	tests := []struct {
		name    string
		args    args
		want    Cluster[uint64, uint64, struct{}]
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
				[]ConnConfig[uint64]{
					{1, "1"},
				},
			},
			&cluster[uint64, uint64, struct{}]{
				list: []Shard[uint64, struct{}]{
					&shard[uint64, struct{}]{1, struct{}{}},
				},
				hash: dh,
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
				[]ConnConfig[uint64]{},
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
				[]ConnConfig[uint64]{
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
				[]ConnConfig[uint64]{
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
				new(dummyHash[uint64]),
				[]ConnConfig[uint64]{
					{1, "1"},
				},
			},
			&cluster[uint64, uint64, struct{}]{
				list: []Shard[uint64, struct{}]{
					&shard[uint64, struct{}]{1, struct{}{}},
				},
				hash: new(dummyHash[uint64]),
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
				[]ConnConfig[uint64]{
					{1, "1"},
				},
			},
			&cluster[uint64, uint64, struct{}]{
				list: []Shard[uint64, struct{}]{
					&shard[uint64, struct{}]{1, struct{}{}},
				},
				hash: dh,
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
				[]ConnConfig[uint64]{
					{2, "2"},
					{3, "3"},
					{1, "1"},
				},
			},
			&cluster[uint64, uint64, struct{}]{
				list: []Shard[uint64, struct{}]{
					&shard[uint64, struct{}]{1, struct{}{}},
					&shard[uint64, struct{}]{2, struct{}{}},
					&shard[uint64, struct{}]{3, struct{}{}},
				},
				hash: dh,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Connect(tt.args.ctx, tt.args.connect, tt.args.hash, tt.args.configs...)
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
		list []Shard[uint64, struct{}]
	}
	tests := []struct {
		name   string
		fields fields
		want   []Shard[uint64, struct{}]
	}{
		{
			"1",
			fields{[]Shard[uint64, struct{}]{
				&shard[uint64, struct{}]{0, struct{}{}},
			}},
			[]Shard[uint64, struct{}]{
				&shard[uint64, struct{}]{0, struct{}{}},
			},
		},
		{
			"2",
			fields{[]Shard[uint64, struct{}]{
				&shard[uint64, struct{}]{0, struct{}{}},
				&shard[uint64, struct{}]{1, struct{}{}},
			}},
			[]Shard[uint64, struct{}]{
				&shard[uint64, struct{}]{0, struct{}{}},
				&shard[uint64, struct{}]{1, struct{}{}},
			},
		},
		{
			"3",
			fields{[]Shard[uint64, struct{}]{
				&shard[uint64, struct{}]{0, struct{}{}},
				&shard[uint64, struct{}]{1, struct{}{}},
				&shard[uint64, struct{}]{2, struct{}{}},
			}},
			[]Shard[uint64, struct{}]{
				&shard[uint64, struct{}]{0, struct{}{}},
				&shard[uint64, struct{}]{1, struct{}{}},
				&shard[uint64, struct{}]{2, struct{}{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, uint64, struct{}]{
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
		list []Shard[uint64, struct{}]
		hash Hash[uint64]
	}
	type args struct {
		id uint64
	}
	ff := fields{
		list: []Shard[uint64, struct{}]{
			&shard[uint64, struct{}]{1, struct{}{}},
			&shard[uint64, struct{}]{2, struct{}{}},
			&shard[uint64, struct{}]{3, struct{}{}},
		},
		hash: newDefaultHash[uint64](),
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint64
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
			c := &cluster[uint64, uint64, struct{}]{
				list: tt.fields.list,
				hash: tt.fields.hash,
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
				if got := newDefaultHash[int64]().Sum(tt.args.int64Id); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			case "uint64":
				if got := newDefaultHash[uint64]().Sum(tt.args.uint64Id); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			case "string":
				if got := newDefaultHash[string]().Sum(tt.args.stringId); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			case "bytes":
				if got := newDefaultHash[[]byte]().Sum(tt.args.bytesId); got != tt.want {
					t.Errorf("defaultHash() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func Test_shard_Conn(t *testing.T) {
	type fields struct {
		id   uint64
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
			s := &shard[uint64, struct{}]{
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
		id   uint64
		conn struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		{"1", fields{1, struct{}{}}, 1},
		{"2", fields{2, struct{}{}}, 2},
		{"3", fields{3, struct{}{}}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard[uint64, struct{}]{
				id:   tt.fields.id,
				conn: tt.fields.conn,
			}
			if got := s.ID(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ItemID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Map(t *testing.T) {
	sh := []Shard[uint64, struct{}]{
		&shard[uint64, struct{}]{1, struct{}{}},
		&shard[uint64, struct{}]{2, struct{}{}},
		&shard[uint64, struct{}]{3, struct{}{}},
	}
	dh := newDefaultHash[uint64]()
	type fields struct {
		list []Shard[uint64, struct{}]
		hash Hash[uint64]
	}
	type args struct {
		ids []uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[Shard[uint64, struct{}]][]uint64
	}{
		{
			"1",
			fields{sh, dh},
			args{ids: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			map[Shard[uint64, struct{}]][]uint64{
				sh[0]: {1, 7, 10},
				sh[1]: {4, 9},
				sh[2]: {2, 3, 5, 6, 8},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, uint64, struct{}]{
				list: tt.fields.list,
				hash: tt.fields.hash,
			}
			if got := c.Map(tt.args.ids); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Map() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Each(t *testing.T) {
	sh := []Shard[uint64, struct{}]{
		&shard[uint64, struct{}]{1, struct{}{}},
		&shard[uint64, struct{}]{2, struct{}{}},
		&shard[uint64, struct{}]{3, struct{}{}},
	}
	dh := newDefaultHash[uint64]()
	type fields struct {
		list []Shard[uint64, struct{}]
		hash Hash[uint64]
	}
	type args struct {
		fn func(s Shard[uint64, struct{}]) error
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
			args{func(s Shard[uint64, struct{}]) error {
				return nil
			}},
			false,
		},
		{
			"1",
			fields{sh, dh},
			args{func(s Shard[uint64, struct{}]) error {
				return errors.New("error")
			}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cluster[uint64, uint64, struct{}]{
				list: tt.fields.list,
				hash: tt.fields.hash,
			}
			if err := c.Each(tt.args.fn); (err != nil) != tt.wantErr {
				t.Errorf("Each() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_cluster_ByKey(t *testing.T) {
	sh := []Shard[uint64, struct{}]{
		&shard[uint64, struct{}]{1, struct{}{}},
		&shard[uint64, struct{}]{2, struct{}{}},
		&shard[uint64, struct{}]{3, struct{}{}},
	}
	dh := newDefaultHash[uint64]()
	type fields struct {
		list []Shard[uint64, struct{}]
		hash Hash[uint64]
	}
	type args struct {
		ids []uint64
		fn  func([]uint64, Shard[uint64, struct{}]) error
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
				func(ids []uint64, s Shard[uint64, struct{}]) error {
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
				func(ids []uint64, s Shard[uint64, struct{}]) error {
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
			c := &cluster[uint64, uint64, struct{}]{
				list: tt.fields.list,
				hash: tt.fields.hash,
			}
			if err := c.ByKey(tt.args.ids, tt.args.fn); (err != nil) != tt.wantErr {
				t.Errorf("ByKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
