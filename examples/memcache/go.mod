module github.com/skamenetskiy/sharding/examples/memcache

go 1.19

replace github.com/skamenetskiy/sharding => ../../

require (
	github.com/bradfitz/gomemcache v0.0.0-20220106215444-fb4bf637b56d
	github.com/skamenetskiy/sharding v0.0.0-00010101000000-000000000000
)

require github.com/rs/xid v1.4.0 // indirect
