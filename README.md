# sharding [![test](https://github.com/skamenetskiy/sharding/actions/workflows/go.yml/badge.svg)](https://github.com/skamenetskiy/sharding/actions/workflows/go.yml) [![Coverage Status](https://coveralls.io/repos/github/skamenetskiy/sharding/badge.svg?branch=main)](https://coveralls.io/github/skamenetskiy/sharding?branch=main) [![CodeQL](https://github.com/skamenetskiy/sharding/actions/workflows/codeql.yml/badge.svg)](https://github.com/skamenetskiy/sharding/actions/workflows/codeql.yml) [![report](https://goreportcard.com/badge/github.com/skamenetskiy/sharding)](https://goreportcard.com/report/github.com/skamenetskiy/sharding) [![godoc](https://godoc.org/github.com/skamenetskiy/sharding?status.svg)](http://godoc.org/github.com/skamenetskiy/sharding)

⚡ Super light and extremely flexible sharding library written in pure go.

## Features

- 🆓 _Dependency free_ - uses only standard go packages but can be extended with customs if required.
- 🌐 _Generic_ - unlike most libraries, not limited to `sql.DB` or any other interface, you define the client.
- 🤵 _Elegant_ - modern looking API, generic, lambdas and other cool stuff.
- 💯 _Tested_ - 100% test coverage (and planning to keep it this way).

## Examples

- [SQL](examples/sql/main.go)
- [Memcache](examples/memcache/main.go)

## Pitfalls

- ❌ Key types are currently limited to `string`, `int64`, `uint64` or `[]byte`.
- ❌ There's no re-sharding support. It is too specific for each use case, thus pretty hard to implement.

## License

The _sharding_ package is open-sourced software licensed under the [MIT license](LICENSE).