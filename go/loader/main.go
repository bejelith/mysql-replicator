package loader

import (
	"fmt"
	"github.com/juju/loggo"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

var (
	defaultTable = "test"
	intIndex     = 0
	log          = loggo.GetLogger("loader")
)

type MySQLLoader interface {
	ExecFunc(func(conn *client.Conn) error) error
	Exec(string, ...interface{}) (*mysql.Result, error)
	ExecBatch([]string) error
	Begin() error
	Commit() error
	Rollback() error
	Position() (string, uint64)
	GTid() (mysql.GTIDSet, error)
	SetAutocommit(bool) error
	Close() error
}

type mySQLLoader struct {
	client *client.Conn
}

func NewDefaultLoader() (MySQLLoader, error) {
	return NewLoader("127.0.0.1", 3306, "root", "", "test")
}

func NewLoader(host string, port int, user string, passwd string, db string) (MySQLLoader, error) {

	conn, err := client.Connect(fmt.Sprintf("%s:%d", host, port), user, passwd, "mysql")
	if err != nil {
		return nil, err
	}
	_ = conn.SetAutoCommit()

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	if _, err := conn.Execute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)); err != nil {
		return nil, err
	}

	if err := conn.UseDB(db); err != nil {
		return nil, err
	}

	instance := &mySQLLoader{
		conn,
	}
	return MySQLLoader(instance), nil
}

func (l *mySQLLoader) Begin() error {
	return l.client.Begin()
}

func (l *mySQLLoader) Commit() error {
	return l.client.Commit()
}

func (l *mySQLLoader) Rollback() error {
	return l.client.Rollback()
}

func (l *mySQLLoader) SetAutocommit(b bool) error {
	if b {
		return l.client.SetAutoCommit()
	} else {
		if _, err := l.client.Execute("SET AUTOCOMMIT = 0"); err != nil {
			return err
		}
	}
	return nil
}

func (l *mySQLLoader) Exec(query string, args ...interface{}) (*mysql.Result, error) {
	if len(args) == 0 {
		return l.client.Execute(query)
	}
	return l.client.Execute(query, args)
}

func (l *mySQLLoader) ExecFunc(f func(client *client.Conn) error) error {
	return f(l.client)
}

func (l *mySQLLoader) ExecBatch(queries []string) error {
	for _, q := range queries {
		if _, err := l.client.Execute(q); err != nil {
			return fmt.Errorf("Error running \"%s\": %v", q, err)
		}
	}
	return nil
}

func (l *mySQLLoader) Position() (string, uint64) {
	var currentLog string = ""
	var currentPos uint64 = 0
	_ = l.ExecFunc(func(conn *client.Conn) error {
		if res, _ := conn.Execute("SHOW MASTER STATUS;"); len(res.Values) > 0 {
			currentLog, currentPos = string(res.Values[0][0].([]uint8)), res.Values[0][1].(uint64)
		}
		return nil
	})
	return currentLog, currentPos
}

func (l *mySQLLoader) GTid() (mysql.GTIDSet, error) {
	r, err := l.client.Execute("show global variables like 'gtid_executed';")
	gtid, _ := mysql.ParseGTIDSet("mysql", string(r.Values[0][1].([]uint8)))
	return gtid, err
}

func (l *mySQLLoader) Close() error {
	return l.client.Close()
}

func (l *mySQLLoader) insertInts() error {
	for i := 0; i < 30; i++ {
		query := fmt.Sprintf("INSERT INTO table (id, data) VALUES (%d, 'DATA');", intIndex)
		intIndex += 1
		if _, err := l.client.Execute(query); err != nil {

			return err
		}

	}
	return nil
}
