package godatapipe

import (
	"database/sql"
	"os"
	"strconv"

	"github.com/juju/errors"
)

type Config struct {
	MaxRowBufSz    int //Maximum number of rows to buffer at a time
	MaxRowTxCommit int //Maximum number of rows to process before committing the database transaction

	SrcConn      *sql.DB // Source database connection overrides Driver/Uri
	SrcDbDriver  string  //Source database driver name
	SrcDbUri     string  //Source database driver URI
	SrcSelectSql string  //Source database select SQL statement

	DstConn     *sql.DB // Destination database connection overrides Driver/Uri
	DstDbDriver string  //Destination database driver name
	DstDbUri    string  //Destination database driver URI
	DstSchema   string
	DstTable    string //Destination database table name

	ShowStackTrace bool //Display stack traces on error
}

func (c *Config) Init() (err error) {
	if os.Getenv("SHOW_STACK_TRACE") != "" {
		c.ShowStackTrace = true
	}

	c.MaxRowBufSz, _ = c.EnvInt("MAX_ROW_BUF_SZ", 100)
	c.MaxRowTxCommit, _ = c.EnvInt("MAX_ROW_TX_COMMIT", 500)

	if c.SrcDbDriver, err = c.EnvStr("SRC_DB_DRIVER"); err != nil {
		return errors.Trace(err)
	}
	if c.SrcDbUri, err = c.EnvStr("SRC_DB_URI"); err != nil {
		return errors.Trace(err)
	}
	if c.SrcSelectSql, err = c.EnvStr("SRC_DB_SELECT_SQL"); err != nil {
		return errors.Trace(err)
	}

	if c.DstDbDriver, err = c.EnvStr("DST_DB_DRIVER"); err != nil {
		return errors.Trace(err)
	}
	if c.DstDbUri, err = c.EnvStr("DST_DB_URI"); err != nil {
		return errors.Trace(err)
	}
	if c.DstSchema, err = c.EnvStr("DST_DB_SCHEMA"); err != nil {
		return errors.Trace(err)
	}
	if c.DstTable, err = c.EnvStr("DST_DB_TABLE"); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *Config) EnvStr(envName string) (dst string, err error) {
	dst = os.Getenv(envName)
	if dst == "" {
		err = errors.Errorf("Missing ENV variable: %s", envName)
	}

	return dst, err
}

func (c *Config) EnvInt(envName string, defaultValue int) (dst int, err error) {
	if dst, err = strconv.Atoi(os.Getenv(envName)); err != nil {
		dst = defaultValue
	}

	return dst, nil
}
