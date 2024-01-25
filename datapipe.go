package godatapipe

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5"
	"github.com/joescharf/go-datapipe/bulk"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/xo/dburl"

	"github.com/juju/errors"
)

type Insert interface {
	Append(rows *sql.Rows) (err error)
	Flush() (totalRowCount int, err error)
	Close() (err error)
}

func Run(cfg *Config) (rowCount int, err error) {
	var srcDb, dstDb *sql.DB
	srcDBurl, err := dburl.Parse(cfg.SrcDbUri)

	dstDBurl, err := dburl.Parse(cfg.DstDbUri)

	// If we don't already have a connection...
	if cfg.SrcConn == nil {
		if srcDb, err = sql.Open(srcDBurl.Driver, srcDBurl.DSN); err != nil {
			return 0, errors.Trace(err)
		}
	} else {
		srcDb = cfg.SrcConn
	}

	defer srcDb.Close()

	if cfg.DstConn == nil {
		if dstDb, err = sql.Open(dstDBurl.Driver, dstDBurl.DSN); err != nil {
			return 0, errors.Trace(err)
		}
	} else {
		dstDb = cfg.DstConn
	}

	defer dstDb.Close()

	if err = clearTable(dstDb, cfg); err != nil {
		return 0, errors.Trace(err)
	}

	if rowCount, err = copyTable(srcDb, dstDb, cfg); err != nil {
		return 0, errors.Trace(err)
	}

	return rowCount, nil
}

func clearTable(dstDb *sql.DB, cfg *Config) (err error) {
	if _, err = dstDb.Exec("TRUNCATE TABLE " + fqSchemaTable(cfg.DstSchema, cfg.DstTable)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// fqSchemaTable concatenates schema and table
// taking into account a null schema
func fqSchemaTable(schema string, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func copyTable(srcDb *sql.DB, dstDb *sql.DB, cfg *Config) (rowCount int, err error) {
	var ir Insert
	var rows *sql.Rows
	var columns []string

	readStart := time.Now()

	if rows, err = srcDb.Query(cfg.SrcSelectSql); err != nil {
		return 0, errors.Trace(err)
	}

	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return 0, errors.Trace(err)
	}

	readEnd := time.Since(readStart)
	writeStart := time.Now()

	switch cfg.DstDbDriver {
	case "postgres":
		if ir, err = bulk.NewCopyIn(dstDb, columns, cfg.DstSchema, cfg.DstTable); err != nil {
			return 0, errors.Trace(err)
		}
	default:
		if ir, err = bulk.NewBulk(
			dstDb, columns,
			cfg.DstSchema, cfg.DstTable,
			cfg.MaxRowBufSz, cfg.MaxRowTxCommit); err != nil {
			return 0, errors.Trace(err)
		}
	}

	rowCount, err = copyBulkRows(dstDb, rows, ir, cfg)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err = ir.Close(); err != nil {
		return 0, errors.Trace(err)
	}

	writeEnd := time.Since(writeStart)

	fmt.Printf("%d rows queried in %s and inserted in %s\n",
		rowCount,
		readEnd.String(),
		writeEnd.String())

	return rowCount, errors.Trace(rows.Err())
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
