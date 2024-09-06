package mysql

import (
	"bytes"
	"context"
	"fmt"
	"github.com/juju/loggo"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"
	"time"
)

var(
	mysqlProcess MySQL
	log = loggo.GetLogger("mysql")
)

type MySQL struct {
	proc        *os.Process
	state		*os.ProcessState
	initialized bool
}

func writeMycnd(mycnf string) error {
	os.MkdirAll("/mysqldata", os.ModePerm)
	cmd := exec.Command("chown","-R", "mysql:mysql", "/mysqldata")
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Run(); err != nil{
		fmt.Printf("Error chanding ownership of %s: %s\n", "/mysqldata\n%s\n", err, cmd.Stderr)
	}
	return ioutil.WriteFile("/etc/mysql/my.cnf", []byte(mycnf), os.ModePerm)
}

func Mycnf() string {
	return `[mysqld]
server-id = 1
log-error = /mysqldata/error.log
binlog_format = ROW
binlog-ignore-db = mysql
datadir = /mysqldata
log-bin = bin.log
gtid-mode = ON
enforce_gtid_consistency = ON
`
}

func RestartMySQL() (*os.Process, error){
	log.Infof("Restart MySQL instance")
	StopMySQL()
	return StartMySQL()
}

func StopMySQL(){
	mysqlProcess.proc.Signal(syscall.SIGTERM)
	time.Sleep(300 * time.Millisecond)
	if !mysqlProcess.state.Exited() {
		mysqlProcess.proc.Signal(syscall.SIGKILL)	
	}
}

func StartMySQL() (*os.Process, error) {
	log.Infof("Starting MySQL")
	safe_cmd := exec.Command("/usr/bin/mysqld_safe")
	if err := safe_cmd.Start(); err != nil{
		return nil, err
	}
	mysqlProcess.proc = safe_cmd.Process
	mysqlProcess.state = safe_cmd.ProcessState
	
	if err:=ApplyRootPrivileges(); err!=nil{
		return nil, err
	}
	return safe_cmd.Process, nil
}

func InitMySQL(configprovider func() string) error {
	if mysqlProcess.initialized {
		return nil
	}
	log.Infof("Initializing Mysql database")
	writeMycnd(configprovider())
	safe_cmd := exec.Command("/usr/bin/mysqld_safe","--initialize-insecure")
	if err := safe_cmd.Run(); err != nil {
		mysqlProcess.initialized = false
		return err
	}
	mysqlProcess.initialized = true
	return nil
}

func ApplyRootPrivileges() error {
	query := "update mysql.user set host='%' where user = 'root'; flush privileges;"
	
	log.Infof("Setting up privileges...",)
	
	ctx, _ :=context.WithDeadline(context.Background(), time.Now().Add(45 * time.Second))
	var err error
	for {
		select{
		case <-ctx.Done():
			return fmt.Errorf("Privileges setup timed out")
		default:
			c := exec.Command("mysql", "-e", query)
			err = c.Run()
			if err == nil {
				log.Infof("Privileges set!")
				return nil
			}else{
				log.Errorf("Error connecting to mysql: %v", err)
			}
			
		}
		time.Sleep(250 * time.Millisecond)
	}
}
