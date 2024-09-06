package replicator

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"math/rand"
	"testing"
)

type MockLoader struct {
	begin    int
	commit   int
	rollback int
	position int
	exec     int
}

func (l *MockLoader) ExecFunc(f func(conn *client.Conn) error) error {
	l.exec++
	return nil
}

func (l *MockLoader) Exec(string, ...interface{}) (*mysql.Result, error) {
	l.exec++
	return nil, nil
}

func (l *MockLoader) ExecBatch([]string) error {
	l.exec++
	return nil
}

func (l *MockLoader) Begin() error {
	l.begin++
	return nil
}

func (l *MockLoader) Rollback() error {
	l.rollback++
	return nil
}

func (l *MockLoader) Commit() error {
	l.commit++
	return nil
}

func (l *MockLoader) Position() (string, uint64) {
	return "fakefile", uint64(l.position)
}

func (l *MockLoader) GTid() (mysql.GTIDSet, error) {
	set, _ := mysql.ParseGTIDSet("", "")
	return set, nil
}

func (l *MockLoader) SetAutocommit(autocommit bool) error {
	return nil
}

func (l *MockLoader) Close() error {
	return nil
}

func TestSetGetGTID(t *testing.T) {
	handler := NewWdHandler(&MockLoader{})
	if handler.LastCommittedGITD() != nil {
		t.Fatal("GTID should be nil if no transactions have been received")
	}
	set := mysql.GTIDSet(&mysql.MysqlGTIDSet{})
	handler.SetGITD(&set)
	if handler.LastCommittedGITD() == nil {
		t.Fatal("GTID should not be nil if no transactions have been received")
	}

}

func TestSetGetPosition(t *testing.T) {
	loader := &MockLoader{}
	handler := NewWdHandler(loader)
	for i := 0; i < 5; i++ {
		if err := handler.OnRow(&canal.RowsEvent{}); err != nil {
			t.Fatalf("Unexpected error from OnRow %s", err)
		}
	}
	if err := handler.OnPosSynced(mysql.Position{"logname", 100}, true); err != nil {
		t.Fatalf("Unexpected error from OnRow %s", err)
	}
	pos := handler.LastCommittedPos()
	if pos.Name != "logname" {
		t.Fatal("Received wrong log name")
	}
	if pos.Pos != 100 {
		t.Fatal("Received wrong log position")
	}
}

func TestOnPosSyncFailsIfNoTransactionExists(t *testing.T) {
	loader := &MockLoader{}
	handler := NewWdHandler(loader)
	if err := handler.OnPosSynced(mysql.Position{"logname", 100}, true); err == nil {
		t.Fatal("Should have no syncronized positon for a ghost transaction")
	}
}

func TestOverlappingDDLAndDML(t *testing.T) {
	handler := NewWdHandler(&MockLoader{})
	handler.OnRow(&canal.RowsEvent{})
	if err := handler.OnDDL(mysql.Position{}, &replication.QueryEvent{}); err == nil {
		t.Fatalf("Expected error on DDL during existing transaction")
	}
}

func TestStandartTransaction(t *testing.T) {
	loader := &MockLoader{}
	handler := NewWdHandler(loader)
	rows := 1 + rand.Int()&200
	for i := 0; i < rows; i++ {
		if err := handler.OnRow(&canal.RowsEvent{}); err != nil {
			t.Fatalf("Unexpected error from OnRow %s", err)
		}
	}
	if loader.begin != 1 {
		t.Fatalf("Wrong count of 'begin' statements for transaction: %d", loader.begin)
	}
	if loader.commit != 0 {
		t.Fatalf("Wrong count of 'commits' statements for transaction: %d", loader.commit)
	}
	if loader.exec != rows {
		t.Fatalf("Wrong count of row events statements for transaction, %d", loader.exec)
	}
	if err := handler.OnPosSynced(mysql.Position{}, true); err != nil {
		t.Fatalf("Error committing transaction, %v", err)
	}
	if err := handler.OnPosSynced(mysql.Position{}, true); err == nil {
		t.Fatalf("Double commit for same transaction should not happen, %v", err)
	}
}
