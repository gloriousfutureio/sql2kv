package sql2kv

import (
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func setup() error {
	log.Println("Running setup")

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
		return err
	}

	err = dropTables(db)
	if err != nil {
		return err
	}

	err = setupUserTable(db)
	if err != nil {
		return err
	}

	return err

}

func dropTables(db *sqlx.DB) error {
	_, err := db.Exec("DROP TABLE users;")
	if err != nil {
		return err
	}

	return nil

}

func setupUserTable(db *sqlx.DB) error {

	schema := `CREATE TABLE users (
	id integer auto_increment primary key,
    name text,
    age integer NULL
    );`

	_, err := db.Exec(schema)
	if err != nil {
		return err
	}

	users := []struct {
		query string
		name  string
		age   int
	}{
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Farhan",
			32,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Coleman",
			32,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Jeff May",
			27,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Mr&MissGophie",
			1,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Dogs",
			200,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"GopherThing",
			20,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Linus Torval",
			100,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Rob Pike",
			100,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"NinjaMan",
			23232,
		},
		{
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"Zack",
			34,
		},
	}

	for _, s := range users {
		_, err := db.Exec(s.query, s.name, s.age)
		if err != nil {
			return err

		}

	}

	rows, err := db.Query("SELECT * FROM test.users")
	if err != nil {
		return err
	}

	c := 0

	for rows.Next() {
		c++
	}

	if c != 10 {
		return errors.New("Missing correct number of rows")
	}

	return nil
}
