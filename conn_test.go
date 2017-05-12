package sql2kv

import (
	"testing"
)

func TestMySQL(t *testing.T) {

	conf := MySQLConfig{
		Username: "root",
		Password: "test",
		Schema:   "test",
		Host:     "localhost",
		Port:     "3306",
		Params:   "",
		Trust:    "",
	}

	db, err := NewMySQLConn(conf)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	// Get lots of stuff from information_schema
	_ = `select table_name from information_schema.columns limit 5`

	rows, err := db.Queryx(`
	select 
	TABLE_SCHEMA,
	TABLE_NAME,
	COLUMN_NAME,
	DATA_TYPE
	IS_NULLABLE,
	COLUMN_KEY
	from information_schema.columns where TABLE_SCHEMA='test'`)

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	for rows.Next() {

		// Probably need to model this explicitly
		type InfoSchema struct {
			Database   string `db:"TABLE_SCHEMA"`
			TableName  string `db:"TABLE_NAME"`
			ColumnName string `db:"COLUMN_NAME"`
			DataType   string `db:"DATA_TYPE"`
			IsNullable string `db:"IS_NULLABLE"`
			ColumnKey  string `db:"COLUMN_KEY"`
		}

		_, err := rows.Columns()
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		var is InfoSchema
		if rows.StructScan(&is); err != nil {
			t.Error(err)
		}
	}

}
