package sql2kv

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Info Schema for mysql
type ColumnSchema struct {
	Database   string `db:"TABLE_SCHEMA"`
	TableName  string `db:"TABLE_NAME"`
	ColumnName string `db:"COLUMN_NAME"`
	DataType   string `db:"DATA_TYPE"`
	IsNullable string `db:"IS_NULLABLE"`
	ColumnKey  string `db:"COLUMN_KEY"`
}

type TableSchema struct {
	Name       string
	Columns    []ColumnSchema
	PrimaryKey string
}

func GetTableSchemas(db *sqlx.DB, schema string, tables []string) ([]TableSchema, error) {

	var results []TableSchema

	for _, t := range tables {
		q := fmt.Sprintf(`Select 
		TABLE_SCHEMA,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		IS_NULLABLE,
		COLUMN_KEY
		FROM information_schema.columns
		WHERE TABLE_NAME='%s';`, t)

		tableSchema := TableSchema{t, nil, ""}

		rows, err := db.Queryx(q)
		if err != nil {
			return nil, err

			for rows.Next() {
				var c ColumnSchema

				if rows.StructScan(&c); err != nil {
					if c.ColumnKey == "PRI" {
						tableSchema.PrimaryKey = c.ColumnKey
					}

					tableSchema.Columns = append(tableSchema.Columns, c)
				}
			}
		}

		results = append(results, tableSchema)

	}

	return results, nil
}
