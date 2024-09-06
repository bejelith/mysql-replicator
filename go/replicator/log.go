package replicator

import "github.com/juju/loggo"


// Configure package level logging
var log = loggo.GetLogger("replicator")

func SetLogger(logger loggo.Logger){
	log = logger
}
