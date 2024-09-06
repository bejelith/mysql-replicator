package replicator

import (
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"mysqlreplicator/loader"
)

type DefaultWDHandler interface {
	canal.EventHandler
	LastCommittedGITD() *mysql.GTIDSet
	LastCommittedPos() *mysql.Position
	SetGITD(*mysql.GTIDSet)
	SetPos(*mysql.Position)
}

type defaultWDHandler struct {
	canal.DummyEventHandler
	position           *mysql.Position
	gtid               *mysql.GTIDSet
	inTransaction      bool
	currentTransaction []*canal.RowsEvent
	client             loader.MySQLLoader
}

func NewWdHandler(loader loader.MySQLLoader) DefaultWDHandler {
	return &defaultWDHandler{
		client: loader,
	}
}

func (h *defaultWDHandler) LastCommittedGITD() *mysql.GTIDSet {
	return h.gtid
}

func (h *defaultWDHandler) LastCommittedPos() *mysql.Position {
	return h.position
}

func (h *defaultWDHandler) SetGITD(set *mysql.GTIDSet) {
	h.gtid = set
}

func (h *defaultWDHandler) SetPos(pos *mysql.Position) {
	h.position = pos
}

func (h *defaultWDHandler) Close() error {
	return h.client.Close()
}

func (h *defaultWDHandler) OnRow(ev *canal.RowsEvent) error {
	if !h.inTransaction {
		if err := h.client.Begin(); err != nil {
			return err
		}
		h.inTransaction = true
	}
	if _, err := h.client.Exec(""); err != nil {
		h.client.Rollback()
		return err
	}
	return nil
}

func (e *defaultWDHandler) OnRotate(ev *replication.RotateEvent) error {
	e.position = &mysql.Position{string(ev.NextLogName), uint32(ev.Position)}
	return nil
}
func (e *defaultWDHandler) OnTableChanged(schema string, table string) error {
	return nil
}

func (e *defaultWDHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if e.inTransaction {
		return fmt.Errorf("Current transaction has not ended, unexpected DDL query received")
	}
	e.inTransaction = true // DDL is always a transaction
	return nil
}

func (e *defaultWDHandler) OnPosSynced(position mysql.Position, force bool) error {
	if !e.inTransaction {
		return fmt.Errorf("No transaction to commit")
	}
	if err := e.client.Commit(); err != nil {
		e.client.Rollback()
		return err
	}
	e.position = &position
	e.inTransaction = false
	return nil
}
