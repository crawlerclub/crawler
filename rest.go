package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"time"

	"crawler.club/et"
	"github.com/golang/glog"
	"zliu.org/goutil/rest"
)

var (
	addr = flag.String("addr", ":2001", "rest address")
)

func AddTaskHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infof("addr=%s  method=%s host=%s uri=%s",
		r.RemoteAddr, r.Method, r.Host, r.RequestURI)
	r.ParseForm()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		rest.MustEncode(w, rest.RestMessage{"ERROR", err.Error()})
		return
	}
	var task = new(et.UrlTask)
	if err = json.Unmarshal(b, task); err != nil {
		rest.MustEncode(w, rest.RestMessage{"ERROR", err.Error()})
		return
	}
	task.TaskName = time.Now().Format("200601020304")
	k := taskKey(task)
	if has, err := dedupStore.Has(k); has {
		rest.MustEncode(w, rest.RestMessage{"DUP", k})
		return
	} else if err != nil {
		rest.MustEncode(w, rest.RestMessage{"ERROR", err.Error()})
		return
	}
	dedupStore.Put(k, nil)
	b, _ = json.Marshal(task)
	if err = crawlTopic.Push(string(b)); err != nil {
		rest.MustEncode(w, rest.RestMessage{"ERROR", err.Error()})
		return
	}
	rest.MustEncode(w, rest.RestMessage{"OK", k})
}

func web() {
	if crawlTopic == nil || dedupStore == nil {
		glog.Error("topics did not init")
		return
	}
	http.Handle("/api/addtask", rest.WithLog(AddTaskHandler))
	glog.Info("rest server listen on", *addr)
	glog.Error(http.ListenAndServe(*addr, nil))
}
