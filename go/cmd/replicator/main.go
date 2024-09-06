package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	repl "github.com/siddontang/go-mysql/replication"
	"os"
)

var (
	a            int = 3
	mysql_host       = flag.String("host", "127.0.0.1", "MySQL Host")
	mysql_port       = flag.Int("port", 3306, "MySQL Port")
	mysql_id         = flag.Int("id", 100, "MySQL Port")
	mysql_user       = flag.String("user", "root", "MySQL User")
	mysql_passwd     = flag.String("passwd", "root", "MySQL Password")
)

func main() {
	//connstring := fmt.Sprintf("%s:%d", *mysql_host, *mysql_port)
	//conn, err := client.Connect(connstring, *mysql_user, *mysql_passwd, "test2")
	//if err != nil{
	//	fmt.Printf("%s", connstring)
	//	fmt.Printf("%v", err)
	//	os.Exit(1)
	//}
	//conn.Ping()
	cfg := repl.BinlogSyncerConfig{
		ServerID: uint32(*mysql_id),
		Flavor:   "mysql",
		Host:     *mysql_host,
		Port:     uint16(*mysql_port),
		User:     *mysql_user,
		Password: *mysql_passwd,
	}
	syncer := repl.NewBinlogSyncer(cfg)
	//set, _ := mysql.ParseMysqlGTIDSet("")
	streamer, err := syncer.StartSync(mysql.Position{})
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)

	}

	for {
		ev, err := streamer.GetEvent(context.Background())
		// Dump event
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		//if ev.Header.EventType == repl.QUERY_EVENT {
		//	qe := ev.Event.(*repl.QueryEvent)
		//
		//	query:= string(qe.Query)
		//	if query != "BEGIN" && string(qe.Schema) == "test" {
		//		fmt.Println(string(qe.Query))
		//		conn.Execute(string(qe.Query))
		//	}
		//}
		ev.Dump(os.Stdout)
	}
}
