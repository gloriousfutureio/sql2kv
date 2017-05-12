package sql2kv

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestGetTableSchema(t *testing.T) {

	ts := []struct {
		name string
		in   string
		out  *TableSchema
	}{
		{

			"Testing Basic User Schema",
			"users",
			&TableSchema{
				"test",
				"users",
				[]ColumnSchema{
					{"test", "users", "id", "int", "NO", "PRI", 1},
					{"test", "users", "name", "text", "YES", "", 2},
					{"test", "users", "age", "int", "YES", "", 3},
				},
				"id",
			},
		},
	}

	for _, ts := range ts {

		actual, err := GetTableSchema(testMySQLDB, "test", ts.in)
		if err != nil {
			t.Errorf("Get Table Schema Failed due to %v", err)
		}

		if !reflect.DeepEqual(ts.out, actual) {
			t.Errorf("\n Expected: %v \n, Acutal: %v \n", ts.out, actual)
			t.FailNow()

		}
	}
}

func TestQueryTableAndWriteKV(t *testing.T) {

	var ts = TableSchema{
		"test",
		"users",
		[]ColumnSchema{
			{"test", "users", "id", "int", "NO", "PRI", 1},
			{"test", "users", "name", "text", "YES", "", 2},
			{"test", "users", "age", "int", "YES", "", 3},
		},
		"id",
	}

	rows, err := QueryTable(testMySQLDB, ts)
	if err != nil {
		t.Errorf("error from QueryTable %v", err)
	}

	row := rows[0]

	if row["name"].(string) != "Farhan" {
		t.Errorf("Expected name Farhan got %s", row["name"].(string))
	}
	if row["age"].(int64) != 32 {
		t.Errorf("Expected age 32 got %v", row["row"].(int64))
	}

	err = WriteKV(testLevelDB, "test", "users", row["__pk"].(string), "!", row)
	if err != nil {
		t.Errorf("error from WriteKV: %v", err)
	}

	k := []byte("test!users!1")

	found, err := testLevelDB.Get(k, nil)
	if err != nil {
		t.Errorf("could not find key: %v", err)
	}

	var unmarshaled map[string]interface{}
	if err := json.Unmarshal(found, &unmarshaled); err != nil {
		t.Error(err)
	}

	if unmarshaled["name"].(string) != "Farhan" {
		t.Errorf("expected to unmarshal name Farhan, got: %s", unmarshaled["name"].(string))
	}

	// JSON only marhsals as float64 for type numeric.
	if unmarshaled["age"].(float64) != 32 {
		t.Errorf("expected to unmarshal age 32, got: %v", unmarshaled["age"].(float64))
	}

	// Test string representation of our special key, the primary key.
	if unmarshaled["__pk"].(string) != "1" {
		t.Errorf("Expected __pk to be 1, got: %s", unmarshaled["__pk"].(string))
	}
}
