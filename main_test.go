package sql2kv

import (
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func setup() {
	log.Println("Running setup")

}
