package sql2kv

import (
	"encoding/json"
	"reflect"
	"testing"
)

var TableSchemaFixture = TableSchema{
	"test",
	"users",
	UserColumnSchemaFixture,
	"id",
}

// UserFixture for tests
var UserColumnSchemaFixture = []ColumnSchema{
	{"test", "users", "id", "int", "NO", "PRI", 1},
	{"test", "users", "name", "text", "YES", "", 2},
	{"test", "users", "age", "int", "YES", "", 3},
	{"test", "users", "hint", "varchar", "YES", "", 4},
}

// UserJsonFixture for tests
var UserJsonFixture = []map[string]interface{}{
	{
		"name": "Farhan",
		"age":  int64(32),
		"id":   int64(1),
		"hint": "hi",
		"__pk": "1",
	},
	{
		"name": "Coleman",
		"age":  int64(32),
		"id":   int64(2),
		"hint": "hi",
		"__pk": "2",
	},
	{
		"name": "Jeff May",
		"age":  int64(27),
		"id":   int64(3),
		"hint": "hi",
		"__pk": "3",
	},
	{
		"name": "Mr&MissGophie",
		"age":  int64(1),
		"id":   int64(4),
		"hint": "hi",
		"__pk": "4",
	},
	{
		"name": "Dogs",
		"age":  int64(200),
		"id":   int64(5),
		"hint": "hi",
		"__pk": "5",
	},
	{
		"name": "GopherThing",
		"age":  int64(20),
		"id":   int64(6),
		"hint": "hi",
		"__pk": "6",
	},
	{
		"name": "Linus Torval",
		"age":  int64(100),
		"id":   int64(7),
		"hint": "hi",
		"__pk": "7",
	},
	{
		"name": "Rob Pike",
		"age":  int64(100),
		"id":   int64(8),
		"hint": "hi",
		"__pk": "8",
	},
	{
		"name": "NinjaMan",
		"age":  int64(23232),
		"id":   int64(9),
		"hint": "hi",
		"__pk": "9",
	},
	{
		"name": "Zack",
		"age":  int64(34),
		"id":   int64(10),
		"hint": "hi",
		"__pk": "10",
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
			&TableSchemaFixture,
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
		UserColumnSchemaFixture,
		"id",
	}

	var expectRows = UserJsonFixture

	rows, err := QueryTable(testMySQLDB, ts)
	if err != nil {
		t.Errorf("error from QueryTable %v", err)
	}

	for i, r := range rows {

		if r["__pk"] != expectRows[i]["__pk"] {

			t.Errorf("\n Expected %v \n Acutal %v", expectRows[i], r)
			t.FailNow()
		}

	}

}

func TestWriteKV(t *testing.T) {

	ts := []struct {
		in  map[string]interface{}
		key string
		out map[string]interface{}
	}{
		{
			UserJsonFixture[0],
			"test!users!1",
			map[string]interface{}{
				"id":   float64(1),
				"name": "Farhan",
				"age":  float64(32),
				"hint": "hi",
				"__pk": "1",
			},
		},
		{
			UserJsonFixture[1],
			"test!users!2",
			map[string]interface{}{
				"id":   float64(2),
				"name": "Coleman",
				"age":  float64(32),
				"hint": "hi",

				"__pk": "2",
			},
		},
		{
			UserJsonFixture[2],
			"test!users!3",
			map[string]interface{}{
				"id":   float64(3),
				"name": "Jeff May",
				"age":  float64(27),
				"hint": "hi",
				"__pk": "3",
			},
		},
		{
			UserJsonFixture[3],
			"test!users!4",
			map[string]interface{}{
				"id":   float64(4),
				"name": "Mr&MissGophie",
				"age":  float64(1),
				"hint": "hi",
				"__pk": "4",
			},
		},
		{
			UserJsonFixture[4],
			"test!users!5",
			map[string]interface{}{
				"id":   float64(5),
				"name": "Dogs",
				"age":  float64(200),
				"hint": "hi",
				"__pk": "5",
			},
		},
		{
			UserJsonFixture[5],
			"test!users!6",
			map[string]interface{}{
				"id":   float64(6),
				"name": "GopherThing",
				"age":  float64(20),
				"hint": "hi",
				"__pk": "6",
			},
		},
		{
			UserJsonFixture[6],
			"test!users!7",
			map[string]interface{}{
				"id":   float64(7),
				"name": "Linus Torval",
				"age":  float64(100),
				"hint": "hi",
				"__pk": "7",
			},
		},
		{
			UserJsonFixture[7],
			"test!users!8",
			map[string]interface{}{
				"id":   float64(8),
				"name": "Rob Pike",
				"age":  float64(100),
				"hint": "hi",
				"__pk": "8",
			},
		},
		{
			UserJsonFixture[8],
			"test!users!9",
			map[string]interface{}{
				"id":   float64(9),
				"name": "NinjaMan",
				"age":  float64(23232),
				"hint": "hi",
				"__pk": "9",
			},
		},
		{
			UserJsonFixture[9],
			"test!users!10",
			map[string]interface{}{
				"id":   float64(10),
				"name": "Zack",
				"age":  float64(34),
				"hint": "hi",
				"__pk": "10",
			},
		},
	}

	for _, c := range ts {
		err := WriteKV(testLevelDB, TableSchemaFixture, c.in, "!")
		if err != nil {
			t.Errorf("error from WriteKV: %v", err)
		}

		found, err := testLevelDB.Get([]byte(c.key), nil)
		if err != nil {
			t.Errorf("could not find key: %v", err)
		}

		var actual map[string]interface{}
		if err := json.Unmarshal(found, &actual); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(c.out, actual) {

			t.Errorf("\n Expected %v \n Acutal %v", c.out, actual)
			t.FailNow()
		}

	}

}
