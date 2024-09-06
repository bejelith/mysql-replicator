package main

import (
	mysqlctl "containerrunner/mysql"
	logger "github.com/juju/loggo"
	"os"
	"os/exec"
)

var log = logger.GetLogger("main")

func main(){
	logger.GetLogger("").SetLogLevel(logger.DEBUG)
	mysqlctl.InitMySQL(mysqlctl.Mycnf)
	mysqlserver, err := mysqlctl.StartMySQL()
	if err != nil{
		log.Errorf("Failed to start mysql: %v\n", err)
	} else if len(os.Args) > 1 {
		var cmd *exec.Cmd
		log.Infof("Running command %s\n", os.Args[1])
		if len(os.Args) > 2 {
			//args := strings.Join(os.Args[2:]," ")
			log.Infof("Executing command %s", os.Args[1:])
			cmd = exec.Command(os.Args[1], os.Args[2:]...)
		}else{
			cmd = exec.Command(os.Args[1])
		}
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmd.Stdin = os.Stdin
		err := cmd.Run()
		if err != nil{
			log.Errorf("Command failed: %v", err)
			os.Exit(1)
		}
	} else {
		mysqlserver.Wait()
	}
}