package main

import (
	"flag"

	"github.com/golang/glog"
	"zliu.org/q"
)

var (
	addr = flag.String("addr", ":2001", "bind address")
)

func main() {
	flag.Parse()
	defer glog.Flush()

	glog.Info("server listen on ", *addr)
	svr, err := q.NewServer(*addr)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Error(svr.Run())
}
