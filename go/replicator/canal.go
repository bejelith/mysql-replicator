package replicator

import (
	"context"
	"fmt"
	"time"

	ls "github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
)

type State int

const (
	Running = iota
	Stopped
	Terminated
)

type WDCanal interface {
	Start() error
	Stop()
	State() (State, error)
	SetGTID(*mysql.GTIDSet) error
	SetPos(*mysql.Position) error
}

type wdcanal struct {
	context context.Context
	c       *canal.Canal
	error   error
	state   State
	gtid    mysql.GTIDSet
	pos     *mysql.Position
	handler DefaultWDHandler
	config  *canal.Config
}

func (e *wdcanal) Start() error {
	if e.state == Running {
		return fmt.Errorf("Canal is already started")
	}
	
	if e.c, e.error = newCanal(e.config, e.handler); e.error != nil {
		return e.error
	}

	e.error = nil
	e.state = Running

	errChan := e.run()
	//wait for canal to start
	time.Sleep(time.Millisecond * 200)

	select {
	// Context is Done, then check if error was generated
	case <-e.c.Ctx().Done():
		select {
		case err := <-errChan:
			e.state = Terminated
			e.error = err
		default:
			e.state = Stopped
		}
	// Canal returned an error
	case err := <-errChan:
		e.state = Terminated
		e.error = err
	// Do not block when successful
	default:
	}
	return e.error
}

func (e *wdcanal) run() chan error {
	errChan := make(chan error)

	go func() {
		var err error
		if e.handler.LastCommittedPos() != nil {
			log.Infof("Starting from position %v", e.handler.LastCommittedPos())
			err = e.c.RunFrom(*e.handler.LastCommittedPos())
		} else if e.handler.LastCommittedGITD() != nil {
			log.Infof("Starting from GTID %v", e.handler.LastCommittedGITD())
			err = e.c.StartFromGTID(*e.handler.LastCommittedGITD())
		} else {
			errChan <- fmt.Errorf("Not GTID or Position to start from")
		}
		if err != nil {
			errChan <- err
		}
	}()
	return errChan
}

func (e *wdcanal) State() (State, error) {
	return e.state, e.error
}

func (e *wdcanal) Stop() {
	switch e.state {
	case Stopped, Terminated:
	default:
		e.c.Close()
		e.state = Stopped
	}
}

func (e *wdcanal) SetGTID(set *mysql.GTIDSet) error {
	switch e.state {
	case Running:
		return fmt.Errorf("Can not change GTID while canal is running")
	default:
		e.handler.SetGITD(set)
	}
	return nil
}

func (e *wdcanal) SetPos(pos *mysql.Position) error {
	if e.handler == nil {
		return fmt.Errorf("Nil handler %v", e)
	}
	switch e.state {
	case Running:
		return fmt.Errorf("Can not change log position while canal is running")
	default:
		e.handler.SetPos(pos)
	}
	return nil
}

func newConf(server_id uint32, host string, port int, user string, passwd string, handler DefaultWDHandler) *canal.Config {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.User = user
	cfg.Password = passwd
	cfg.Flavor = "mysql"
	cfg.ParseTime = false
	cfg.ServerID = server_id
	cfg.IncludeTableRegex = []string{""}
	cfg.ExcludeTableRegex = []string{"^mysql.*"}
	cfg.TimestampStringLocation = time.FixedZone("UTC", 0)
	
	return cfg
}

func newCanal(config *canal.Config, handler canal.EventHandler) (*canal.Canal, error) {

	newcanal, err := canal.NewCanal(config)
	if err != nil {
		return nil, err
	}
	newcanal.SetEventHandler(handler)
	return newcanal, nil
}

func NewWdCanal(server_id uint32, host string, port int, user string, passwd string, handler DefaultWDHandler) WDCanal {
	ls.SetLevel(ls.LevelFatal)
	config := newConf(server_id, host, port, user, passwd, handler)

	return &wdcanal{
		context: context.Background(),
		state:   Stopped,
		handler: handler,
		config:  config,
	}

}
