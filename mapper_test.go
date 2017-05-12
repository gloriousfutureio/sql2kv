package sql2kv

import (
	"reflect"
	"testing"
)

func TestGetTableSchemas(t *testing.T) {

	ts := []TableSchema{
		TableSchema{"users",
			[]ColumnSchema{
				{"test", "users", "id", "int", "NO", "PRI"},
				{"test", "users", "name", "text", "YES", ""},
				{"test", "users", "age", "int", "YES", ""},
			},
			"id",
		},
	}

	results, err := GetTableSchemas(testMysqlDb, "test", []string{"users", "address"})
	if err != nil {
		t.Errorf("Get Table Schema Failed due to %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected %v got %v", 2, len(results))
		t.FailNow()
	}

	for i, _ := range ts {
		if !reflect.DeepEqual(ts[i], results[i]) {
			t.Errorf("\n Expected: %v \n, Acutal: %v \n", ts[i], results[i])
			t.FailNow()

		}
	}

}
