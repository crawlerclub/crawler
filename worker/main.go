package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"crawler.club/dl"
	"crawler.club/et"
	"github.com/golang/glog"
	"zliu.org/q"
)

var (
	databus = flag.String("databus", "127.0.0.1:2001", "databus address")
	task    = flag.String("task", "crawler_task", "crawler task topic name")
	save    = flag.String("save", "store_item", "crawler store topic name")
	timeout = flag.Int64("timeout", 30, "in seconds")

	taskCh    = make(chan *TaskItem, 100)
	keyCh     = make(chan string)
	contentCh = make(chan interface{})
	newTaskCh = make(chan interface{})
)

type TaskItem struct {
	Task *et.UrlTask
	Key  string
}

func stop(sigs chan os.Signal, exit chan bool) {
	<-sigs
	glog.Info("receive stop signal!")
	close(exit)
}

func dispatch(exit chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := q.NewClient(*databus, *task)
	if err != nil {
		glog.Fatal(err)
	}
	for {
		select {
		case <-exit:
			return
		case err = <-client.Errors:
			glog.Error(err)
			return
		case <-client.Closed:
			glog.Error("connection to databus closed")
			return
		case newTask := <-newTaskCh:
			if err = client.Push(newTask); err != nil {
				glog.Error(err)
				continue
			}
		default:
			key, value, err := client.Pop(*timeout)
			if err != nil {
				continue
			}
			glog.Infof("key:%s, value:%s", key, value)
			item := &TaskItem{new(et.UrlTask), key}
			if err = json.Unmarshal([]byte(value), item.Task); err != nil {
				glog.Error(err)
				continue
			}
			taskCh <- item
		}
	}
}

func confirm(exit chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := q.NewClient(*databus, *task)
	if err != nil {
		glog.Fatal(err)
	}
	for {
		select {
		case <-exit:
			return
		case err = <-client.Errors:
			glog.Error(err)
			return
		case <-client.Closed:
			glog.Error("connection to databus closed")
			return
		case key := <-keyCh:
			glog.Infof("confirm: %s", key)
			if err = client.Confirm(key); err != nil {
			}
		}
	}
}

func collect(exit chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := q.NewClient(*databus, *save)
	if err != nil {
		glog.Fatal(err)
	}
	for {
		select {
		case <-exit:
			return
		case err = <-client.Errors:
			glog.Error(err)
			return
		case <-client.Closed:
			glog.Error("connection to databus closed")
			return
		case content := <-contentCh:
			glog.Infof("push %+v", content)
			if err = client.Push(content); err != nil {
			}
		}
	}
}

func do(exit chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-exit:
			return
		case item := <-taskCh:
			glog.Infof("do: %+v", item)
			resp := dl.DownloadUrl(item.Task.Url)
			if resp.Error != nil {
				glog.Error(resp.Error)
				continue
			}
			tasks, rec, e := et.Parse(item.Task.ParserName, resp.Text, item.Task.Url)
			if e != nil {
				glog.Error(e)
				continue
			}
			for _, t := range tasks {
				newTaskCh <- t
			}
			for _, r := range rec {
				contentCh <- r
			}
		}
	}
}

func main() {
	flag.Parse()
	defer glog.Flush()

	exit := make(chan bool)
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go stop(sigs, exit)

	var wg sync.WaitGroup
	wg.Add(3)
	go dispatch(exit, &wg)
	go confirm(exit, &wg)
	go collect(exit, &wg)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go do(exit, &wg)
	}

	wg.Wait()

	glog.Info("exit!")
}
