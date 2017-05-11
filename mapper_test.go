package sql2kv

import (
	"testing"
)

func TestGetTableSchemas(t *testing.T) {

	tc := []struct {
		T TableSchema
	}{
		TableSchema{
			"users",
			[]ColumnSchema{
				{"test", "users", "age", "int", "", ""},
				{"test", "users", "id", "int", "", "PRI"},
				{"test", "users", "name", "text", "", ""},
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

}
