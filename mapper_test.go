package sql2kv

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// UsersTableSchemaFixture models our "test.users" table.
var UsersTableSchemaFixture = TableSchema{
	"test",
	"users",
	UsersColumnSchemaFixture,
	"id",
}

// UsersColumnSchemaFixture models a subset of INFORMATION_SCHEMA.COLUMNS for
// our "test.users" table.
var UsersColumnSchemaFixture = []ColumnSchema{
	{"test", "users", "id", "int", "NO", "PRI", 1},
	{"test", "users", "name", "text", "YES", "", 2},
	{"test", "users", "age", "int", "YES", "", 3},
	{"test", "users", "hint", "varchar", "YES", "", 4},
	{"test", "users", "alive", "tinyint", "YES", "", 5},
	{"test", "users", "user_key", "binary", "YES", "", 6},
}

// UserJSONSchema models the JSON serializable types for our "test.users" table.
// We expect these results to be written and read as []byte to and from a local
// kv store.
var UserJSONSchema = []map[string]interface{}{
	{
		"name":     "Farhan",
		"age":      int64(32),
		"id":       int64(1),
		"hint":     "hi",
		"alive":    true,
		"user_key": []byte("commonkey"),
		"__pk":     "1",
	},
	{
		"name":     "Coleman",
		"age":      int64(32),
		"id":       int64(2),
		"hint":     "hi",
		"alive":    false,
		"user_key": []byte("commonkey"),
		"__pk":     "2",
	},
	{
		"name":     "Jeff May",
		"age":      int64(27),
		"id":       int64(3),
		"hint":     "hi",
		"alive":    true,
		"user_key": []byte("commonkey"),
		"__pk":     "3",
	},
	{
		"name":     "Mr&MissGophie",
		"age":      int64(1),
		"id":       int64(4),
		"hint":     "hi",
		"alive":    false,
		"user_key": []byte("commonkey"),
		"__pk":     "4",
	},
	{
		"name":     "Dogs",
		"age":      int64(200),
		"id":       int64(5),
		"hint":     "hi",
		"alive":    true,
		"user_key": []byte("commonkey"),
		"__pk":     "5",
	},
	{
		"name":     "GopherThing",
		"age":      int64(20),
		"id":       int64(6),
		"hint":     "hi",
		"alive":    false,
		"user_key": []byte("commonkey"),
		"__pk":     "6",
	},
	{
		"name":     "Linus Torval",
		"age":      int64(100),
		"id":       int64(7),
		"hint":     "hi",
		"alive":    true,
		"user_key": []byte("commonkey"),
		"__pk":     "7",
	},
	{
		"name":     "Rob Pike",
		"age":      int64(100),
		"id":       int64(8),
		"hint":     "hi",
		"alive":    false,
		"user_key": []byte("commonkey"),
		"__pk":     "8",
	},
	{
		"name":     "NinjaMan",
		"age":      int64(23232),
		"id":       int64(9),
		"hint":     "hi",
		"alive":    true,
		"user_key": []byte("commonkey"),
		"__pk":     "9",
	},
	{
		"name":     "Zack",
		"age":      int64(34),
		"id":       int64(10),
		"hint":     "hi",
		"alive":    false,
		"user_key": []byte("commonkey"),
		"__pk":     "10",
	},
}

var KVRepresentation = []map[string]interface{}{
	{

		"id":       float64(1),
		"name":     "Farhan",
		"age":      float64(32),
		"hint":     "hi",
		"alive":    true,
		"user_key": commonKeyBase64,
		"__pk":     "1",
	},
	{
		"id":       float64(2),
		"name":     "Coleman",
		"age":      float64(32),
		"alive":    false,
		"hint":     "hi",
		"user_key": commonKeyBase64,
		"__pk":     "2",
	},
	{
		"id":       float64(3),
		"name":     "Jeff May",
		"age":      float64(27),
		"hint":     "hi",
		"alive":    true,
		"user_key": commonKeyBase64,
		"__pk":     "3",
	},
	{
		"id":       float64(4),
		"name":     "Mr&MissGophie",
		"age":      float64(1),
		"hint":     "hi",
		"alive":    false,
		"user_key": commonKeyBase64,
		"__pk":     "4",
	},
	{
		"id":       float64(5),
		"name":     "Dogs",
		"age":      float64(200),
		"hint":     "hi",
		"alive":    true,
		"user_key": commonKeyBase64,
		"__pk":     "5",
	},
	{
		"id":       float64(6),
		"name":     "GopherThing",
		"age":      float64(20),
		"hint":     "hi",
		"alive":    false,
		"user_key": commonKeyBase64,
		"__pk":     "6",
	},
	{
		"id":       float64(7),
		"name":     "Linus Torval",
		"age":      float64(100),
		"hint":     "hi",
		"alive":    true,
		"user_key": commonKeyBase64,
		"__pk":     "7",
	},
	{
		"id":       float64(8),
		"name":     "Rob Pike",
		"age":      float64(100),
		"hint":     "hi",
		"alive":    false,
		"user_key": commonKeyBase64,
		"__pk":     "8",
	},
	{
		"id":       float64(9),
		"name":     "NinjaMan",
		"age":      float64(23232),
		"hint":     "hi",
		"alive":    true,
		"user_key": commonKeyBase64,
		"__pk":     "9",
	},
	{
		"id":       float64(10),
		"name":     "Zack",
		"age":      float64(34),
		"hint":     "hi",
		"alive":    false,
		"user_key": commonKeyBase64,
		"__pk":     "10",
	},
}

func TestGetTableSchema(t *testing.T) {

	ts := []struct {
		name string
		in   string
		out  *TableSchema
	}{
		{
			"Testing Basic User Schema",
			"users",
			&UsersTableSchemaFixture,
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
	// This is our end to end test.

	var expectRows = UserJSONSchema
	var ts = UsersTableSchemaFixture

	rows, err := QueryTable(testMySQLDB, ts)
	if err != nil {
		t.Errorf("error from QueryTable %v", err)
	}

	for i, row := range rows {
		if row["__pk"] != expectRows[i]["__pk"] {
			t.Errorf("\n Expected %v \n Acutal %v", expectRows[i], row)
			t.FailNow()
		}
	}

	//
	for i, row := range rows {
		if err := WriteKV(testLevelDB, ts, row, "!"); err != nil {
			t.Errorf("error writing row %v to KV: %v", i, err)
		}

		key := []byte(ts.Schema + "!" + ts.Name + "!" + row["__pk"].(string))
		data, err := testLevelDB.Get(key, nil)
		if err != nil {
			t.Errorf("error getting key %v: %v", key, err)
		}
		var gotten map[string]interface{}
		json.Unmarshal(data, &gotten)
		base64Encoded := gotten["user_key"].(string)
		binary, err := base64.StdEncoding.DecodeString(base64Encoded)
		if err != nil {
			t.Errorf("error decoding base64 field user_key: %v", err)
		}
		trimmed := strings.TrimSpace(string(binary))
		if trimmed != "commonkey" {

			t.Error("expected user_key column to be container commonkey as base64")
			t.Errorf("Got %s;", trimmed)
			t.Errorf("len binary from kv:\t %v", len(binary))
			t.Errorf("len trimmed from kv:\t %v", len(trimmed))
			t.Errorf("len hardcoded:\t %v", len([]byte("commonkey")))
		}

	}

}
