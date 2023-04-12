package log_pgsql

import (
	"github.com/infrago/infra"
	"github.com/infrago/log"
	_ "github.com/lib/pq"
)

var (
	DRIVERS = []string{
		"postgresql", "postgres", "pgsql", "pgdb", "pg",
		"cockroachdb", "cockroach", "crdb",
		"timescaledb", "timescale", "tsdb",
	}
)

func Driver() log.Driver {
	return &pgsqlDriver{}
}

func init() {
	dri := Driver()
	for _, key := range DRIVERS {
		infra.Register(key, dri)
	}
}
