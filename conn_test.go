package sql2kv

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMySQL(t *testing.T) {

	conf := MySQLConfig{
		Username: "",
		Password: "",
		Schema:   "",
		Host:     "",
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
			Name string `db:"table_name"`
		}

		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		var is InfoSchema
		_ = is
		m := make(map[string]interface{})
		if rows.MapScan(m); err != nil {
			t.Error(err)
		}
		fmt.Println(cols)
		//fmt.Println(is)
		fmt.Println(m)
		t := reflect.TypeOf(m["table_name"])
		fmt.Println(t.String())
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
