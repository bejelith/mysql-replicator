package mock

import (
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type MockHandler struct {
	canal.DummyEventHandler
	Trasactions   [][]*canal.RowsEvent
	Pos           *mysql.Position
	Commits       []mysql.Position
	Tables        []string
	Gtid          *mysql.GTIDSet
	InTransaction bool
}

func (h *MockHandler) SetGITD(set *mysql.GTIDSet) {
	h.Gtid = set
}

func (h *MockHandler) SetPos(pos *mysql.Position) {
	h.Pos = pos
}

func (h *MockHandler) LastCommittedGITD() *mysql.GTIDSet {
	return h.Gtid
}

func (h *MockHandler) LastCommittedPos() *mysql.Position {
	return h.Pos
}

func (c *MockHandler) OnRow(ev *canal.RowsEvent) error {
	if !c.InTransaction {
		c.Trasactions = append(c.Trasactions, make([]*canal.RowsEvent, 0))
	}

	last := len(c.Trasactions) - 1
	c.Trasactions[last] = append(c.Trasactions[last], ev)
	c.InTransaction = true
	fmt.Printf("ROW %v\n", ev)
	return nil
}

func (c *MockHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	c.Tables = append(c.Tables, string(queryEvent.Query))
	fmt.Printf("DDL %s\n", queryEvent.Query)
	c.InTransaction = true // DDL is a single statement transaction!
	return nil
}

func (c *MockHandler) OnGTID(gtid mysql.GTIDSet) error {
	c.Gtid = &gtid
	return nil
}

func (c *MockHandler) OnPosSynced(position mysql.Position, force bool) error {


	if c.InTransaction {
		fmt.Printf("POS %v\n", position)
		c.Commits = append(c.Commits, position)
		c.InTransaction = false
		c.Pos = &position
	}
	return nil
}
