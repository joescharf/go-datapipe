package godatapipe

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5"
	"github.com/joescharf/go-datapipe/bulk"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/xo/dburl"

	"github.com/juju/errors"
)

type Insert interface {
	Append(ctx context.Context, rows *sql.Rows) (err error)
	Flush(ctx context.Context) (totalRowCount int, err error)
	Close() (err error)
}

func Run(ctx context.Context, cfg *Config) (rowCount int, err error) {
	var srcDb, dstDb *sql.DB
	var srcConn, dstConn *sql.Conn
	srcDBurl, err := dburl.Parse(cfg.SrcDbUri)

	dstDBurl, err := dburl.Parse(cfg.DstDbUri)

	// If we don't already have a connection...
	if cfg.SrcConn == nil {
		if srcDb, err = sql.Open(srcDBurl.Driver, srcDBurl.DSN); err != nil {
			return 0, errors.Trace(err)
		}
		// Only close the connection if we opened it
		defer srcDb.Close()
		// Get the a DB conn.
		srcConn, err = srcDb.Conn(ctx)

	} else {
		srcConn = cfg.SrcConn
	}

	if cfg.DstConn == nil {
		if dstDb, err = sql.Open(dstDBurl.Driver, dstDBurl.DSN); err != nil {
			return 0, errors.Trace(err)
		}
		// Only close the connection if we opened it
		defer dstDb.Close()
		// Get a DB conn
		dstConn, err = dstDb.Conn(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
	} else {
		dstConn = cfg.DstConn
	}

	if err = clearTable(ctx, dstConn, cfg); err != nil {
		return 0, errors.Trace(err)
	}

	if rowCount, err = copyTable(ctx, srcConn, dstConn, cfg); err != nil {
		return 0, errors.Trace(err)
	}

	// TODO: Rowcount is being doubled for some reason
	return rowCount / 2, nil
}

func clearTable(ctx context.Context, dstConn *sql.Conn, cfg *Config) (err error) {
	q := fmt.Sprintf("TRUNCATE TABLE %s", fqSchemaTable(cfg.DstSchema, cfg.DstTable))
	if _, err = dstConn.ExecContext(ctx, q); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// fqSchemaTable concatenates schema and table
// taking into account a null schema and whether the schema and table
// are already quoted.
func fqSchemaTable(schema string, table string) string {
	schemaQuoted := strings.HasPrefix(schema, "`") && strings.HasSuffix(schema, "`")
	tableQuoted := strings.HasPrefix(table, "`") && strings.HasSuffix(table, "`")

	if schema == "" {
		if !tableQuoted {
			table = fmt.Sprintf("`%s`", table)
		}
		return table
	} else {
		if !schemaQuoted {
			schema = fmt.Sprintf("`%s`", schema)
		}
		if !tableQuoted {
			table = fmt.Sprintf("`%s`", table)
		}
		return fmt.Sprintf("%s.%s", schema, table)
	}
}

func copyTable(ctx context.Context, srcConn *sql.Conn, dstConn *sql.Conn, cfg *Config) (rowCount int, err error) {
	var ir Insert
	var rows *sql.Rows
	var columns []string

	readStart := time.Now()

	if rows, err = srcConn.QueryContext(ctx, cfg.SrcSelectSql); err != nil {
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
		if ir, err = bulk.NewCopyIn(ctx, dstConn, columns, cfg.DstSchema, cfg.DstTable); err != nil {
			return 0, errors.Trace(err)
		}
	default:
		if ir, err = bulk.NewBulk(
			ctx, dstConn, columns,
			cfg.DstSchema, cfg.DstTable,
			cfg.MaxRowBufSz, cfg.MaxRowTxCommit); err != nil {
			return 0, errors.Trace(err)
		}
	}

	rowCount, err = copyBulkRows(ctx, dstConn, rows, ir, cfg)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err = ir.Close(); err != nil {
		return 0, errors.Trace(err)
	}

	writeEnd := time.Since(writeStart)

	_ = writeEnd
	_ = readEnd

	// fmt.Printf("%d rows queried in %s and inserted in %s\n",
	// 	rowCount,
	// 	readEnd.String(),
	// 	writeEnd.String())

	return rowCount, errors.Trace(rows.Err())
}

func copyBulkRows(ctx context.Context, dstDb *sql.Conn, rows *sql.Rows, ir Insert, cfg *Config) (rowCount int, err error) {
	var totalRowCount int
	const dotLimit = 1000

	i := 1

	for rows.Next() {
		if err = ir.Append(ctx, rows); err != nil {
			return 0, errors.Trace(err)
		}

		if i%dotLimit == 0 {
			// fmt.Print(".")
			i = 1
		}

		i++
	}

	if totalRowCount, err = ir.Flush(ctx); err != nil {
		return 0, errors.Trace(err)
	}

	if totalRowCount > dotLimit {
		// fmt.Println()
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
