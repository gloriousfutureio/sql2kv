package sql2kv

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/syndtr/goleveldb/leveldb"
	"gopkg.in/yaml.v2"
)

// Config is our top-level app configuration.
type Config struct {
	MySQL   MySQLConfig   `yaml:"mysql"`
	LevelDB LevelDBConfig `yaml:"leveldb"`
}

// NewConfig reads a config file from disk and returns a Config.
func NewConfig(path string) (Config, error) {
	var c Config
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return c, err
	}
	return c, nil
}

// MySQLConfig holds our MySQL config.
type MySQLConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Params   string `yaml:"conn_params"`
	Trust    string `yaml:"trust"`
	Cert     string `yaml:"cert"`
	Key      string `yaml:"key"`
}

// LevelDBConfig ...
type LevelDBConfig struct {
	Path      string `yaml:"path"`
	SizeLimit string `yaml:"size_limit"`
}

func NewMySQLConn(conf MySQLConfig) (*sqlx.DB, error) {

	if conf.Trust != "" {

		tlsConf, err := newTLSClientConfig(conf.Trust, conf.Cert, conf.Key)
		if err != nil {
			return nil, err
		}

		mysql.RegisterTLSConfig("custom", tlsConf)
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?%s",
		conf.Username,
		conf.Password,
		conf.Host,
		conf.Port,
		conf.Schema,
		conf.Params,
	)

	return sqlx.Open("mysql", dsn)
}

func newTLSClientConfig(trustPath, certPath, keyPath string) (*tls.Config, error) {

	trustBytes, err := ioutil.ReadFile(trustPath)
	if err != nil {
		return nil, fmt.Errorf("error parsing CA trust %s: %v", trustPath, err)
	}
	trustCertPool := x509.NewCertPool()
	if !trustCertPool.AppendCertsFromPEM(trustBytes) {
		return nil, fmt.Errorf("error adding CA trust to pool: %v", err)
	}
	cfg := tls.Config{
		ClientCAs:          trustCertPool,
		RootCAs:            trustCertPool,
		InsecureSkipVerify: true,
	}
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("error parsing cert: %v", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
		cfg.BuildNameToCertificate()
	}

	return &cfg, nil
}

func MySQL(db *sqlx.DB, ldb *leveldb.DB) {

}
