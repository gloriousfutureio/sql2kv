package sql2kv

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/syndtr/goleveldb/leveldb"
)

// Config holds our config.
type MySQLConfig struct {
	// MySQL config
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	Schema           string `yaml:"schema"`
	Host             string `yaml:"host"`
	Port             string `yaml:"port"`
	Params           string `yaml:"conn_params"`
	Trust, Cert, Key string

	LevelDBConfig
}

// LevelDBConfig ...
type LevelDBConfig struct {
	Path      string `yaml:"leveldb_path"`
	SizeLimit string `yaml:"leveldb_size_limit"`
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
