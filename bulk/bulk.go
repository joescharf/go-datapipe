package bulk

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
)

type Bulk struct {
	conn *sql.Conn //Database handle
	tx   *sql.Tx

	schema         string
	tableName      string
	columns        []string
	maxRowTxCommit int

	stmt   *sql.Stmt     //Prepared statement for bulk insert
	buf    []interface{} //Buffer to hold values to insert
	bufSz  int           //Size of the buffer
	bufPos int

	valuePtrs []interface{} //Pointer to current row buffer
	values    []interface{} //Buffer for the current row
	colCount  int           //Number of columns

	rowPos        int //Position of current row
	totalRowCount int //Total number of rows
}

// Appends row values to internal buffer
func (r *Bulk) Append(ctx context.Context, rows *sql.Rows) (err error) {
	rows.Scan(r.valuePtrs...)

	//Copy row values into buffer
	for i := 0; i < r.colCount; i++ {
		r.buf[r.bufPos] = r.values[i]
		r.bufPos++
	}

	r.rowPos++
	r.totalRowCount++

	// Need to check if tx is nil (caused if totalRowCount > 0 maxRowTxCommit = 1 )
	if r.tx != nil && r.totalRowCount > 0 && r.totalRowCount%r.maxRowTxCommit == 0 {
		if err = r.tx.Commit(); err != nil {
			return errors.Trace(err)
		}
		r.tx = nil
	}

	//Insert rows if buffer is full
	if r.bufPos >= r.bufSz {
		if r.tx == nil {
			if r.tx, err = r.conn.BeginTx(ctx, nil); err != nil {
				return errors.Trace(err)
			}
		}

		if _, err = r.stmt.Exec(r.buf...); err != nil {
			return errors.Trace(err)
		}

		r.bufPos = 0
		r.rowPos = 0
	}

	return nil
}

// Closes any prepared statements
func (r *Bulk) Close() (err error) {
	if r.stmt != nil {
		r.stmt.Close()
	}

	return nil
}

// Writes any unsaved values from buffer to database
func (r *Bulk) Flush(ctx context.Context) (totalRowCount int, err error) {
	if r.bufPos > 0 {
		buf := make([]interface{}, r.bufPos)
		for i := 0; i < r.bufPos; i++ {
			buf[i] = r.buf[i]
		}

		stmt, err := r.prepare(ctx, r.rowPos)
		if err != nil {
			return 0, errors.Trace(err)
		}

		defer stmt.Close()

		if _, err = stmt.Exec(buf...); err != nil {
			return 0, errors.Trace(err)
		}

		r.totalRowCount += r.rowPos

		r.bufPos = 0
		r.rowPos = 0
	}

	// Source db was empty so we ended up with no rows, and nil tx
	// so we need to test for nil tx otherwise we'll panic.
	if r.tx != nil {
		if err = r.tx.Commit(); err != nil {
			return 0, errors.Trace(err)
		}
	}

	return r.totalRowCount, nil
}

// fqSchemaTable concatenates schema and table
// taking into account a null schema and whether the schema and table
// are already quoted.
func (r *Bulk) FqSchemaTable(schema string, table string) string {
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

// Creates a bulk insert SQL prepared statement based on a number of rows
// Uses $x for row position
func (r *Bulk) prepare(ctx context.Context, rowCount int) (stmt *sql.Stmt, err error) {
	var buf bytes.Buffer

	buf.WriteString("INSERT INTO ")
	buf.WriteString(r.FqSchemaTable(r.schema, r.tableName))
	buf.WriteString(" (")
	for i := 0; i < r.colCount; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(r.columns[i])
	}
	buf.WriteString(") VALUES ")

	pos := 1

	for i := 0; i < rowCount; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(")
		for j := 0; j < r.colCount; j++ {
			if j > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("?") // Placeholder for Mysql
			// buf.WriteString(strconv.Itoa(pos))
			pos++
		}
		buf.WriteString(")")
	}

	return r.conn.PrepareContext(ctx, buf.String())
}

func NewBulk(ctx context.Context, db *sql.Conn, columns []string, schema string, tableName string, rowCount int, maxRowTxCommit int) (r *Bulk, err error) {
	r = &Bulk{
		conn:           db,
		schema:         schema,
		tableName:      tableName,
		columns:        columns,
		maxRowTxCommit: maxRowTxCommit}

	r.colCount = len(columns)

	r.values = make([]interface{}, r.colCount)
	r.valuePtrs = make([]interface{}, r.colCount)

	for i := 0; i < r.colCount; i++ {
		r.valuePtrs[i] = &r.values[i]
	}

	r.bufSz = r.colCount * rowCount
	r.bufPos = 0
	r.rowPos = 0

	r.buf = make([]interface{}, r.bufSz)

	if r.stmt, err = r.prepare(ctx, rowCount); err != nil {
		return nil, errors.Trace(err)
	}

	return r, nil
}
