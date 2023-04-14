package log_pgsql

import (
	"database/sql"
	"fmt"
	"strings"

	. "github.com/infrago/base"
	"github.com/infrago/log"
)

var (
	SCHEMAS = []string{
		"postgresql://",
		"postgres://",
		"pgsql://",
		"pgdb://",
		"cockroachdb://",
		"cockroach://",
		"crdb://",
		"timescale://",
		"timescaledb://",
		"tsdb://",
	}
)

type (
	pgsqlDriver struct {
	}
	pgsqlConnect struct {
		instance *log.Instance
		setting  pgsqlSetting

		db *sql.DB
	}
	pgsqlSetting struct {
		Url    string
		Schema string
		Table  string
	}
)

func (driver *pgsqlDriver) Connect(inst *log.Instance) (log.Connect, error) {
	setting := pgsqlSetting{
		Url:    "postgres://127.0.0.1:5432/log?sslmode=disable",
		Schema: "public", Table: "logs",
	}

	if vv, ok := inst.Setting["url"].(string); ok && vv != "" {
		setting.Url = vv
	}
	if vv, ok := inst.Setting["schema"].(string); ok && vv != "" {
		setting.Schema = vv
	}
	if vv, ok := inst.Setting["table"].(string); ok && vv != "" {
		setting.Table = vv
	}

	//支持自定义的schema，相当于数据库名
	for _, s := range SCHEMAS {
		if strings.HasPrefix(setting.Url, s) {
			setting.Url = strings.Replace(setting.Url, s, "postgres://", 1)
		}
	}

	return &pgsqlConnect{
		instance: inst, setting: setting,
	}, nil
}

// 打开连接
func (this *pgsqlConnect) Open() error {
	db, err := sql.Open("postgres", this.setting.Url)
	if err != nil {
		return err
	}

	this.db = db

	return nil
}

// 关闭连接
func (this *pgsqlConnect) Close() error {
	if this.db != nil {
		return this.db.Close()
	}

	return nil
}

// Write 写日志
func (this *pgsqlConnect) Write(msgs ...log.Log) error {
	vals := []Any{}

	lines := []string{}
	for i, msg := range msgs {
		params := []string{}
		for j := 1; j <= 5; j++ {
			params = append(params, fmt.Sprintf("$%d", i*5+j))
		}
		lines = append(lines, strings.Join(params, ","))

		data := msg.Mapping()
		vals = append(vals, data["time"], data["name"], data["role"], data["level"], data["body"])
	}

	sql := fmt.Sprintf(`INSERT INTO "%s"."%s" ("time","name","role","level","body") VALUES (%s)`, this.setting.Schema, this.setting.Table, strings.Join(lines, "),\n("))
	_, err := this.db.Exec(sql, vals...)
	if err != nil {
		return err
	}

	return nil
}
