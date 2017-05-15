package sql2kv

import "testing"
import "reflect"

func TestNewConfig(t *testing.T) {
	var expected = Config{
		MySQLConfig{
			"root",
			"test",
			"test",
			"localhost",
			"3316",
			"",
			"",
			"",
			"foo",
		},
		LevelDBConfig{
			"changeme",
			"100",
		},
	}

	conf, err := NewConfig("testfixtures/test-config.yml")
	if err != nil {
		t.Errorf("could not load config: %v", err)
		t.FailNow()
	}
	if !reflect.DeepEqual(expected, conf) {
		t.Error("testfixtures/test-config.yml does not match expected")
	}
}
