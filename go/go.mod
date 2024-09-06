module mysqlreplicator

require (
	github.com/juju/loggo v0.0.0-20190212223446-d976af380377
	github.com/kr/pretty v0.1.0 // indirect
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8 // indirect
	github.com/pingcap/errors v0.11.0
	github.com/pkg/errors v0.8.1 // indirect
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07
	github.com/siddontang/go-mysql v0.0.0-20190312052122-c6ab05a85eb8
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

replace github.com/siddontang/go-mysql => github.com/bejelith/go-mysql v0.0.0-20190502030731-833b578fe169
