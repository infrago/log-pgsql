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

func (this *pgsqlConnect) Write(msg log.Log) error {
	data := msg.Mapping()

	vals := []Any{
		data["time"], data["name"], data["role"],
		data["level"], data["body"],
	}

	sql := fmt.Sprintf(`INSERT INTO "%s"."%s" ("time","name","role","level","body") VALUES ($1,$2,$3,$4,$5)`, this.setting.Schema, this.setting.Table)
	_, err := this.db.Exec(sql, vals...)
	if err != nil {
		return err
	}

	return nil
}

func (this *pgsqlConnect) Flush() {

}
