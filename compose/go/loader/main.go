package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/loggo"
	"io"
	"os"
	"reflect"
	"strings"
)

var (
	log  = loggo.GetLogger("main")
	root = loggo.GetLogger("")

	dbhost = flag.String("host", "127.0.0.1", "Database host or IP, default 127.0.0.1")
	dbname = flag.String("db", "mysql", "Database name")
	dbuser = flag.String("user", "root", "Database user, default root")
	dbpass = flag.String("passwd", "", "Database password")
	dbport = flag.Int("port", 3306, "Database port, default 3306")
)

func main() {
	flag.Parse()
	root.SetLogLevel(loggo.DEBUG)

	db, err := openMysqlConnection(buildConnectionString(dbhost, dbuser, dbpass, dbname))
	if err != nil {
		log.Errorf("Unable to enstablish database connection %v", err)
		os.Exit(1)
	}
	defer db.Close()
	stdinreader := bufio.NewReader(os.Stdin)
	for line, _ := stdinreader.ReadString('\n'); err != io.EOF; line, _ = stdinreader.ReadString('\n') {
		log.Infof("Running query %s", string(line))
		result := make(map[int][]interface{}, 0)
		query := strings.Trim(line," ")
		res, queryerror := db.Query(query)
		if queryerror != nil {
			log.Errorf("Unable to run query %v", queryerror)
			os.Exit(1)
		}
		columns, _ := res.Columns()
		//types, _ := res.ColumnTypes()
		for i := range columns {
			result[i] = make([]interface{}, 0)
		}
		log.Infof("Reading result")
		for i := 0; res.Next(); i += 1 {
			pointers := make([]interface{}, len(columns))
			values := make([]interface{}, len(columns))
			for i, _ := range values {
				pointers[i] = &values[i]
			}
			_ = res.Scan(pointers...)

			for i, v := range values {
				result[i] = append(result[i], v)
			}

		}

		for row := 0; row < len(result[0]); row += 1 {
			for col := 0; col < len(columns); col += 1 {
				value := result[col][row]
				t := reflect.TypeOf(value);
				if t == nil {
					fmt.Printf("NULL,")
				} else {
					switch t.Kind() {
					case reflect.Int32, reflect.Int64, reflect.Int:
						fmt.Printf("%d,", value)
					case reflect.String, reflect.Ptr:
						fmt.Printf("%s,", value)
					case reflect.Uint8:
						fmt.Printf("%d,", value)
					case reflect.Slice:
						fmt.Printf("%s,", string(value.([]byte)))
					default:
						fmt.Printf("%s,", reflect.TypeOf(value).Kind())
					}
				}
			}
			fmt.Printf("\n")
		}

	}
	log.Infof("Finishing")

}

func openMysqlConnection(connectionString string) (*sql.DB, error) {
	return sql.Open("mysql", connectionString)
}

func buildConnectionString(dbhost *string, dbuser *string, dbpass *string, dbname *string) string {
	str := fmt.Sprintf("%s@tcp(%s:%d)/%s", buildUserPassword(dbuser, dbpass), *dbhost, *dbport, *dbname)
	return str
}

func buildUserPassword(dbuser *string, dbpass *string) string {
	if dbpass == nil || *dbpass == "" {
		return *dbuser
	}
	return fmt.Sprintf("%s:%s", *dbuser, *dbpass)
}
