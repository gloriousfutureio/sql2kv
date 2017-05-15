package sql2kv

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Globals
var (
	testMySQLDB *sqlx.DB
	testLevelDB *leveldb.DB
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

	dir, err := ioutil.TempDir("", "testLevelDB")
	if err != nil {
		return err
	}

	opts := opt.Options{
		ErrorIfExist: true,
	}
	if testLevelDB, err = leveldb.OpenFile(dir, &opts); err != nil {
		return err
	}

	log.Println("Running setup")

	conf := MySQLConfig{
		Username: "root",
		Password: "test",
		Schema:   "test",
		Host:     "localhost",
		Port:     "3316",
		Params:   "",
		Trust:    "",
	}

	if testMySQLDB, err = NewMySQLConn(conf); err != nil {
		return err
	}

	if err := dropTables(testMySQLDB); err != nil {
		return err
	}

	if err := setupUserTable(testMySQLDB); err != nil {
		return err
	}

	/*
		err = setupAddressTable(testMySQLDB)
		if err != nil {
			return err
		}

		return err
	*/

	return err

}

func dropTables(db *sqlx.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS users;")
	if err != nil {
		return err
	}

	/*
		_, err = db.Exec("DROP TABLE IF EXISTS addresses;")
		if err != nil {
			return err
		}

	*/

	return nil

}

func setupUserTable(db *sqlx.DB) error {

	schema := `CREATE TABLE users (
	id integer auto_increment primary key,
    name text,
    age integer NULL,
	hint varchar(10) NULL,
	alive boolean
    );`

	_, err := db.Exec(schema)
	if err != nil {
		return err
	}

	insertStatement := "INSERT INTO users (name, age, hint, alive) VALUES (?, ?, ?, ?)"

	users := []struct {
		query string
		name  string
		age   int
		hint  string
		alive bool
	}{
		{
			insertStatement,
			"Farhan",
			32,
			"hi",
			true,
		},
		{
			insertStatement,
			"Coleman",
			32,
			"hi",
			false,
		},
		{
			insertStatement,
			"Jeff May",
			27,
			"hi",
			true,
		},
		{
			insertStatement,
			"Mr&MissGophie",
			1,
			"hi",
			false,
		},
		{
			insertStatement,
			"Dogs",
			200,
			"hi",
			true,
		},
		{
			insertStatement,
			"GopherThing",
			20,
			"hi",
			false,
		},
		{
			insertStatement,
			"Linus Torval",
			100,
			"hi",
			true,
		},
		{
			insertStatement,
			"Rob Pike",
			100,
			"hi",
			false,
		},
		{
			insertStatement,
			"NinjaMan",
			23232,
			"hi",
			true,
		},
		{
			insertStatement,
			"Zack",
			34,
			"hi",
			false,
		},
	}

	for _, s := range users {
		_, err := db.Exec(s.query, s.name, s.age, s.hint, s.alive)
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

func setupAddressTable(db *sqlx.DB) error {
	schema := `CREATE TABLE addresses (
	id integer auto_increment primary key,
	street text,
	apt text NULL,
	zip integer,
	state text,
	country text,
	FOREIGN KEY fk_user(id)
	REFERENCES users(id)
	ON DELETE CASCADE
    );`

	_, err := db.Exec(schema)
	if err != nil {
		return err
	}

	adrs := []struct {
		query   string
		street  string
		apt     string
		zip     int
		state   string
		country string
		fk      int
	}{
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1111 abc street",
			"Apt 100",
			11111,
			"DC",
			"USA",
			1,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			11111,
			"FL",
			"USA",
			2,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			11111,
			"FL",
			"USA",
			3,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			11111,
			"FL",
			"USA",
			4,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			11111,
			"FL",
			"USA",
			5,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			11111,
			"FL",
			"USA",
			6,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			11111,
			"FL",
			"USA",
			7,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			993729,
			"FL",
			"USA",
			8,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"1234 abc street",
			"Apt 700",
			99348,
			"FL",
			"USA",
			9,
		},
		{
			"INSERT INTO addresses (street, apt, zip, state, country, fk_user) VALUES (?, ?, ?, ?, ?, ?)",
			"23342 abc street",
			"Apt 700",
			33422,
			"CA",
			"USA",
			10,
		},
	}

	for _, s := range adrs {
		_, err := db.Exec(s.query, s.street, s.apt, s.zip, s.state, s.country, s.fk)
		if err != nil {
			return err

		}

	}

	rows, err := db.Query("SELECT * FROM test.addresses")
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
