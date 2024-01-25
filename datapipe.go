package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"

	"github.com/juju/errors"

	"github.com/literatesnow/go-datapipe/bulk"
)

type Config struct {
	MaxRowBufSz    int //Maximum number of rows to buffer at a time
	MaxRowTxCommit int //Maximum number of rows to process before committing the database transaction

	SrcDbDriver  string //Source database driver name
	SrcDbUri     string //Source database driver URI
	SrcSelectSql string //Source database select SQL statement

	DstDbDriver string //Destination database driver name
	DstDbUri    string //Destination database driver URI
	DstSchema   string
	DstTable    string //Destination database table name

	ShowStackTrace bool //Display stack traces on error
}

type Insert interface {
	Append(rows *sql.Rows) (err error)
	Flush() (totalRowCount int, err error)
	Close() (err error)
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

// func main() {
// 	cfg := &Config{}
// 	if err := cfg.Init(); err != nil {
// 		showError(cfg, err)
// 		os.Exit(2)

// 	} else if err := Run(cfg); err != nil {
// 		showError(cfg, err)
// 		os.Exit(1)
// 	}
// }

func Run(cfg *Config) (err error) {
	var srcDb, dstDb *sql.DB

	if srcDb, err = sql.Open(cfg.SrcDbDriver, cfg.SrcDbUri); err != nil {
		return errors.Trace(err)
	}

	defer srcDb.Close()

	if dstDb, err = sql.Open(cfg.DstDbDriver, cfg.DstDbUri); err != nil {
		return errors.Trace(err)
	}

	defer dstDb.Close()

	if err = clearTable(dstDb, cfg); err != nil {
		return errors.Trace(err)
	}

	if err = copyTable(srcDb, dstDb, cfg); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func clearTable(dstDb *sql.DB, cfg *Config) (err error) {
	if _, err = dstDb.Exec("TRUNCATE TABLE " + cfg.DstSchema + "." + cfg.DstTable); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func copyTable(srcDb *sql.DB, dstDb *sql.DB, cfg *Config) (err error) {
	var ir Insert
	var rows *sql.Rows
	var rowCount int
	var columns []string

	readStart := time.Now()

	if rows, err = srcDb.Query(cfg.SrcSelectSql); err != nil {
		return errors.Trace(err)
	}

	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return errors.Trace(err)
	}

	readEnd := time.Since(readStart)
	writeStart := time.Now()

	switch cfg.DstDbDriver {
	case "postgres":
		if ir, err = bulk.NewCopyIn(dstDb, columns, cfg.DstSchema, cfg.DstTable); err != nil {
			return errors.Trace(err)
		}
	default:
		if ir, err = bulk.NewBulk(
			dstDb, columns,
			cfg.DstSchema, cfg.DstTable,
			cfg.MaxRowBufSz, cfg.MaxRowTxCommit); err != nil {
			return errors.Trace(err)
		}
	}

	rowCount, err = copyBulkRows(dstDb, rows, ir, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	if err = ir.Close(); err != nil {
		return errors.Trace(err)
	}

	writeEnd := time.Since(writeStart)

	fmt.Printf("%d rows queried in %s and inserted in %s\n",
		rowCount,
		readEnd.String(),
		writeEnd.String())

	return errors.Trace(rows.Err())
}

func copyBulkRows(dstDb *sql.DB, rows *sql.Rows, ir Insert, cfg *Config) (rowCount int, err error) {
	var totalRowCount int
	const dotLimit = 1000

	i := 1

	for rows.Next() {
		if err = ir.Append(rows); err != nil {
			return 0, errors.Trace(err)
		}

		if i%dotLimit == 0 {
			fmt.Print(".")
			i = 1
		}

		i++
	}

	if totalRowCount, err = ir.Flush(); err != nil {
		return 0, errors.Trace(err)
	}

	if totalRowCount > dotLimit {
		fmt.Println()
	}

	return totalRowCount, errors.Trace(rows.Err())
}

func showError(cfg *Config, err error) {
	if cfg.ShowStackTrace {
		fmt.Fprintf(os.Stderr, "%s\n", errors.ErrorStack(err))
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}
}
