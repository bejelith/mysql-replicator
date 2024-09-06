package replicator

import (
	"mysqlreplicator/loader"
	"mysqlreplicator/replicator/mock"
	"testing"
	"time"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

func TestNULLReplica(t *testing.T) {
	var err error
	dataloader, _ := loader.NewDefaultLoader()
	currentLog, currentPos := dataloader.Position()
	t.Logf("Starting at %s:%d", currentLog, currentPos)
	defer dataloader.Close()
	handler := &mock.MockHandler{}
	wdcanal := NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)
	wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)})
	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Unable to start canal: %v", err)
	}
	defer wdcanal.Stop()
	_ = dataloader.ExecFunc(func(conn *client.Conn) error {
		queries := []string{
			"drop table if exists testfullrow ",
			"create table testfullrow (id int(8) primary key, data int(8));",
			"INSERT INTO testfullrow (id, data) values (1,NULL)",
			"UPDATE testfullrow SET DATA=111 WHERE ID =1",
		}
		for _, q := range queries {
			if _, err := conn.Execute(q); err != nil {
				t.Fatal(err)
				return err
			}
		}
		time.Sleep(time.Millisecond * 400)
		return nil
	})

	if len(handler.Trasactions) != 2 {
		t.Fatalf("Wrong transaction count, %d", len(handler.Trasactions))
	}

	if handler.Trasactions[0][0].Rows[0][1] != nil {
		t.Fatalf("Expected nil")
	}

	if handler.Trasactions[1][0].Rows[0][1] != nil {
		t.Fatalf("Expected nil, got %v", handler.Trasactions[1][0].Rows[0][1])
	}

	if handler.Trasactions[1][0].Rows[0][1] != nil {
		t.Fatalf("Expected nil, got %v", handler.Trasactions[1][0].Rows[0][1])
	}

}

func TestFullRowReplica(t *testing.T) {
	var err error
	dataloader, _ := loader.NewDefaultLoader()
	dataloader.SetAutocommit(false)
	currentLog, currentPos := dataloader.Position()
	t.Logf("Starting at %s:%d", currentLog, currentPos)
	defer dataloader.Close()
	handler := &mock.MockHandler{}
	wdcanal := NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)
	wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)})
	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Unable to start canal: %v", err)
	}
	defer wdcanal.Stop()
	_ = dataloader.ExecFunc(func(conn *client.Conn) error {
		queries := []string{
			"begin",
			"drop table if exists testfullrow ",
			"create table testfullrow (id int(8) primary key, data int(8));",
			"commit",
			"INSERT INTO testfullrow (id, data) values (1,123)",
			"commit",
			"UPDATE testfullrow SET DATA=111 WHERE ID =1",
			"commit",
		}
		for _, q := range queries {
			if _, err := conn.Execute(q); err != nil {
				t.Fatal(err)
				return err
			}
		}
		time.Sleep(time.Millisecond * 400)
		return nil
	})

	if len(handler.Commits) != 4 {
		t.Fatalf("Unexpected number of commits %d", len(handler.Commits))
	}

	if len(handler.Trasactions) != 2 {
		t.Fatalf("Wrong transaction count, %d", len(handler.Trasactions))
	}

	if handler.Trasactions[0][0].Rows[0][0].(int32) != 1 || handler.Trasactions[0][0].Rows[0][1].(int32) != 123 {
		t.Fatalf("Expected 1, got %v", handler.Trasactions[0][0].Rows[0][0])
	}

	if handler.Trasactions[1][0].Rows[1][0].(int32) != 1 || handler.Trasactions[1][0].Rows[1][1].(int32) != 111 {
		t.Fatalf("Expected 3, got %v", handler.Trasactions[0][0].Rows[1][0])
	}
}

func TestCoumnOrderingInCanal(t *testing.T) {
	var err error

	dataloader, _ := loader.NewDefaultLoader()
	currentLog, currentPos := dataloader.Position()
	defer dataloader.Close()
	handler := &mock.MockHandler{}
	wdcanal := NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)
	wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)})
	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Unable to start canal: %v", err)
	}
	defer wdcanal.Stop()
	dataloader.ExecFunc(func(conn *client.Conn) error {
		q := "INSERT INTO test (data, id) values (23, 2),(24,3)"
		if _, err := conn.Execute(q); err != nil {
			return nil
		}
		time.Sleep(time.Millisecond * 100)
		return nil
	})

	if handler.Trasactions[0][0].Action != "insert" {
		t.Fatalf("Expected insert")
	}

	if len(handler.Trasactions[0]) != 1 {
		t.Fatalf("Transaction 1 has more than 1 query")
	}

	if handler.Trasactions[0][0].Rows[0][0].(int32) != 2 {
		t.Fatalf("Expected 2, got %v", handler.Trasactions[0][0].Rows[0][0])
	}
	if handler.Trasactions[0][0].Rows[1][0].(int32) != 3 {
		t.Fatalf("Expected 3, got %v", handler.Trasactions[0][0].Rows[1][0])
	}
}

func TestCanalRestart(t *testing.T) {
	var err error
	testLoader, _ := loader.NewDefaultLoader()
	currentLog, currentPos := testLoader.Position()
	gtid, _ := testLoader.GTid()
	testLoader2, _ := loader.NewDefaultLoader()
	defer func() {
		_ = testLoader.Close()
		_ = testLoader2.Close()
	}()

	t.Logf("Starting replicator with log %s and position %d", currentLog, currentPos)
	t.Logf("Starting replicator with GTID %s", gtid)

	//insert := "INSERT INTO test (id, data) values (1,23)"
	handler := &mock.MockHandler{}
	wdcanal := NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)

	if err = wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)}); err != nil {
		t.Fatal(err)
	}
	err = wdcanal.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer wdcanal.Stop()
	go testLoader.ExecFunc(func(conn *client.Conn) error {
		time.Sleep(time.Millisecond * 100)
		for i := 0; i < 20; i++ {
			q := "INSERT INTO test (id, data) values (2,23)"
			if _, err := conn.Execute(q); err != nil {
				return nil
			}
		}
		return nil
	})

	_ = testLoader2.Begin()
	err = testLoader2.ExecFunc(func(conn *client.Conn) error {
		ins := []string{"INSERT INTO test (id, data) values (3,23)", "INSERT INTO test (id, data) values (4,23)"}
		for i := 0; i < 300; i++ {
			if _, err := conn.Execute(ins[i%2]); err != nil {
				return nil
			}
		}
		return nil
	})

	_ = testLoader2.Commit()

	if err != nil {
		t.Fatalf("Unable to load test data %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	wdcanal.Stop()

	queries := 0
	for _, t := range handler.Trasactions {
		for range t {
			queries++
		}
	}
	if queries != 320 {
		t.Fatalf("queries: %d", queries)
	}

	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Could not restart canal %v", err)
	}

	_ = testLoader2.Begin()
	_ = testLoader2.ExecFunc(func(conn *client.Conn) error {
		ins := []string{"INSERT INTO test (id, data) values (3,23)", "INSERT INTO test (id, data) values (4,23)"}
		for i := 0; i < 300; i++ {
			if _, err := conn.Execute(ins[i%2]); err != nil {
				return nil
			}
		}
		return nil
	})
	_ = testLoader2.Commit()

	time.Sleep(time.Millisecond * 300)
	queries = 0
	for _, t := range handler.Trasactions {
		for range t {
			queries++
		}
	}
	if queries != 620 {
		t.Fatalf("queries: %d", queries)
	}
	wdcanal.Stop()

}
