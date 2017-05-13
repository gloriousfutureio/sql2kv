package sql2kv

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/syndtr/goleveldb/leveldb"
)

// ColumnSchema ...
type ColumnSchema struct {
	Database        string `db:"TABLE_SCHEMA"`
	TableName       string `db:"TABLE_NAME"`
	ColumnName      string `db:"COLUMN_NAME"`
	DataType        string `db:"DATA_TYPE"`
	IsNullable      string `db:"IS_NULLABLE"`
	ColumnKey       string `db:"COLUMN_KEY"`
	OrdinalPosition int    `db:"ORDINAL_POSITION"`
}

// TableSchema ...
type TableSchema struct {
	Schema     string
	Name       string
	Columns    []ColumnSchema
	PrimaryKey string
}

// ColumnNames returns a slice of column names. The ordering of this slice
// affects the SQL statement yielded by QueryAll.
func (ts TableSchema) ColumnNames() []string {
	var res []string
	for _, col := range ts.Columns {
		res = append(res, col.ColumnName)
	}
	return res
}

// QueryAll yields a SQL string that selects the entire table.
func (ts TableSchema) QueryAll() string {

	var cols []string
	for _, c := range ts.Columns {
		cols = append(cols, c.ColumnName)
	}

	sql := fmt.Sprintf(`select %s from %s.%s ;`,
		strings.Join(cols, ", "), ts.Schema, ts.Name)
	return sql

}

// GetTableSchema ...
func GetTableSchema(db *sqlx.DB, schema string, table string) (*TableSchema, error) {

	tableSchema := TableSchema{schema, table, nil, ""}

	q := fmt.Sprintf(`Select 
		TABLE_SCHEMA,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		IS_NULLABLE,
		COLUMN_KEY,
		ORDINAL_POSITION
		FROM information_schema.columns
		WHERE TABLE_NAME = '%s'
		AND TABLE_SCHEMA = '%s'
		ORDER BY ORDINAL_POSITION `, table, schema)

	rows, err := db.Queryx(q)
	if err != nil {
		return nil, err
	}

	// iterate over the rows
	for rows.Next() {

		var c ColumnSchema

		err = rows.StructScan(&c)

		if err != nil {
			return nil, err
		}

		// Current way to check if a column is a primary key
		// might need to add more or totally refactor this stuff
		if c.ColumnKey == "PRI" {
			tableSchema.PrimaryKey = c.ColumnName
		}

		tableSchema.Columns = append(tableSchema.Columns, c)
	}

	return &tableSchema, nil
}

// QueryTable ...
func QueryTable(db *sqlx.DB, ts TableSchema) ([]map[string]interface{}, error) {

	var res []map[string]interface{}

	sql := ts.QueryAll()

	rows, err := db.Queryx(sql)
	if err != nil {
		return nil, err
	}

	for rows.Next() {

		var newRow []interface{}

		//
		for _, t := range ts.Columns {
			switch t.DataType {
			case "text":
				var d string
				newRow = append(newRow, &d)
			case "int":
				var d int
				newRow = append(newRow, &d)
			default:
				log.Fatal("bad data type")
			}
		}

		err = rows.Scan(newRow...)

		var m = make(map[string]interface{})

		for i, item := range newRow {
			switch item.(type) {
			case *int:
				m[ts.ColumnNames()[i]] = reflect.ValueOf(item).Elem().Int()
			case *string:
				m[ts.ColumnNames()[i]] = reflect.ValueOf(item).Elem().String()
			default:
				log.Println("unhandled type")
			}
			if ts.ColumnNames()[i] == ts.PrimaryKey {
				// If this column is the PK; assign it again to a special key in our map
				m["__pk"] = fmt.Sprintf("%v", m[ts.ColumnNames()[i]])
			}

		}

		res = append(res, m)

	}

	return res, nil

}

// WriteKV writes data to levelDB.
func WriteKV(ldb *leveldb.DB, schema, table, pk, sep string, data map[string]interface{}) error {

	key := []byte(strings.Join([]string{schema, table, pk}, sep))

	value, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return ldb.Put(key, value, nil)

}
