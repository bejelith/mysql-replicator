package dmlbuilder

import (
	"fmt"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"mysqlreplicator/loader"
	"mysqlreplicator/replicator"
	"mysqlreplicator/replicator/mock"
	"strings"
	"testing"
	"time"
)

var createTableSpec = "create table testdatatypes (" +
	"id int(8)," +
	"d BLOB," +
	"t TEXT," +
	"f FLOAT(52)," +
	"de DECIMAL(5,2)," +
	"b BIT(32)," +
	"bool BOOLEAN," +
	"datetimeval DATETIME(3)," + // Datetime and Temestamp support fractional time format yyyy-mm-dd HH:MM:SS.sss
	"timestampval TIMESTAMP(3)," +
	"size ENUM('A', 'B', 'C')," +
	"setvals SET('A', 'B', 'C')" +
	");"

var insertCases = [][]string{
	{
		"1", "'this is \xfe a blob with unicode ᛦ'", "'another text'", "9.2", "9.23", "3", "TRUE", "'2001-01-01 13:10:12.998'", "'2001-01-01 10:10:12.999'", "'B'", "'A,B'",
	},
	{
		"65000", "'ᛦᛦᛦ'", "'xx'", fmt.Sprintf("%f", 9.2), fmt.Sprintf("%f", 9.23), "2", "false", "'2001-01-01 13:10:12.998'", "'2001-01-01 10:10:12.999'", "'B'", "'A,B'",
	},
}

func TestDataTypeConversions(t *testing.T) {
	var err error
	dataloader, _ := loader.NewDefaultLoader()
	currentLog, currentPos := dataloader.Position()
	t.Logf("Starting at %s:%d", currentLog, currentPos)
	defer dataloader.Close()
	handler := &mock.MockHandler{}
	wdcanal := replicator.NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)
	_ = wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)})
	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Unable to start canal: %v", err)
	}
	defer wdcanal.Stop()
	_ = dataloader.ExecBatch([]string{
		"drop table if exists testdatatypes;",
		createTableSpec,
		"SET time_zone = '+01:00';", // We assume the orignal data comes from a different timezone, while mysql replication is UTC
	})

	for i, k := 0, 0; i < len(insertCases); i, k = i+1, k+2 {
		insert := fmt.Sprintf("INSERT INTO testdatatypes values (%s)", strings.Join(insertCases[i], ","))
		_, err = dataloader.Exec(insert)
		if err != nil {
			t.Fatalf("Unable to load test data %v, query: %s", err, insert)
		}
		time.Sleep(100 * time.Millisecond)

		transaction := handler.Trasactions[k][0]

		query := GetDML(transaction)
		dataloader.ExecFunc(func(c *client.Conn) error {
			_, _ = dataloader.Exec("SET time_zone = '+00:00';") //Replication uses UTC
			_, e := dataloader.Exec(query)
			time.Sleep(100 * time.Millisecond)
			return e
		})

		transaction2 := handler.Trasactions[k+1][0]

		if fmt.Sprintf("%v", transaction.Rows[0]) != fmt.Sprintf("%v", transaction2.Rows[0]) {
			t.Fatalf("Expected %v \ngot %v", transaction.Rows[0], transaction2.Rows[0])
		}

	}
}

func TestDeleteDMLPK(t *testing.T) {
	var err error
	dataloader, _ := loader.NewDefaultLoader()
	currentLog, currentPos := dataloader.Position()
	t.Logf("Starting at %s:%d", currentLog, currentPos)
	defer dataloader.Close()
	handler := &mock.MockHandler{}
	wdcanal := replicator.NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)
	_ = wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)})
	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Unable to start canal: %v", err)
	}
	defer wdcanal.Stop()
	err = dataloader.ExecBatch([]string{
		"drop table if exists primarykeydelete;",
		"create table primarykeydelete (id int(8) PRIMARY KEY, data int(8))",
		"insert into primarykeydelete VALUES (1,3)",
		"delete from primarykeydelete where id = 1 and data = 3;",
	})
	if err != nil {
		t.Fatalf("Unabled to prepare dataset for test: %v", err)
	}
	expected := "DELETE FROM test.primarykeydelete WHERE id=1"
	time.Sleep(100 * time.Millisecond)

	transaction := handler.Trasactions[1][0]
	if query := GetDML(transaction) ; query != expected {
		t.Fatalf("Wrong Delete %s, expected %s", query, expected)
	}

}

func TestDeleteDML(t *testing.T) {
	var err error
	dataloader, _ := loader.NewDefaultLoader()
	currentLog, currentPos := dataloader.Position()
	t.Logf("Starting at %s:%d", currentLog, currentPos)
	defer dataloader.Close()
	handler := &mock.MockHandler{}
	wdcanal := replicator.NewWdCanal(uint32(100), "127.0.0.1", 3306, "root", "", handler)
	_ = wdcanal.SetPos(&mysql.Position{currentLog, uint32(currentPos)})
	if err = wdcanal.Start(); err != nil {
		t.Fatalf("Unable to start canal: %v", err)
	}
	defer wdcanal.Stop()
	err = dataloader.ExecBatch([]string{
		"drop table if exists primarykeydelete;",
		"create table primarykeydelete (id int(8), data int(8))",
		"insert into primarykeydelete VALUES (1,3)",
		"delete from primarykeydelete where id = 1 and data = 3;",
	})
	if err != nil {
		t.Fatalf("Unabled to prepare dataset for test: %v", err)
	}
	expected := "DELETE FROM test.primarykeydelete WHERE id=1 AND data=3"
	time.Sleep(100 * time.Millisecond)

	transaction := handler.Trasactions[1][0]
	if query := GetDML(transaction); query != expected {
		t.Fatalf("Wrong Delete %s, expected %s", query,expected)
	}

}