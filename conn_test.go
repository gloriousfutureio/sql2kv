package sql2kv

import (
	"fmt"
	"testing"
)

func TestMySQL(t *testing.T) {
	t.Skip()

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

	rows, err := db.Queryx(`select table_name from information_schema.tables limit 5`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	for rows.Next() {

		// Probably need to model this explicitly
		type InfoSchema struct {
			Name string `db:"TABLE_NAME"`
		}

		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		var is InfoSchema
		_ = is
		if rows.StructScan(&is); err != nil {
			t.Error(err)
		}
		fmt.Println(cols)
		fmt.Println(is)
	}

}

/*
 untyped madness


	for rows.Next() {

		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		data := make([]interface{}, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			pointers[i] = &data[i]
		}
		if rows.Scan(pointers...); err != nil {
			t.Error(err)
		}
		fmt.Println(data)
		fmt.Println(cols)
		for _, item := range data {
			if item == nil {
				fmt.Printf("%s ", "got nil")
				continue
			}
			t := reflect.TypeOf(item)
			fmt.Printf("%s ", t.String())
		}
		fmt.Println("")
	}
*/
