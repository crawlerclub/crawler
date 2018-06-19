package main

import (
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"crawler.club/dl"
	"crawler.club/et"
	"github.com/golang/glog"
	"zliu.org/goutil"
)

var (
	dir     = flag.String("dir", "data", "working dir")
	timeout = flag.Int64("timeout", 30, "in seconds")
)

var crawlTopic, storeTopic *TaskTopic
var once sync.Once

func initTopics() {
	once.Do(func() {
		var err error
		if crawlTopic, err = NewTaskTopic("crawl", *dir); err != nil {
			panic(err)
		}
		if storeTopic, err = NewTaskTopic("store", *dir); err != nil {
			panic(err)
		}
	})
}

func stop(sigs chan os.Signal, exit chan bool) {
	<-sigs
	glog.Info("receive stop signal!")
	close(exit)
}

func do(i int, exit chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	glog.Infof("start worker %d", i)
	for {
		select {
		case <-exit:
			glog.Infof("worker %d exit", i)
			return
		default:
			key, item, err := crawlTopic.Pop(*timeout)
			if err != nil {
				if err.Error() == "Queue is empty" {
					s := rand.Int()%20 + 5
					glog.Infof("queue is empty, worker %d sleep %d seconds", i, s)
					goutil.Sleep(time.Duration(s)*time.Second, exit)
				} else {
					glog.Error(err)
				}
				continue
			}
			task := new(et.UrlTask)
			if err = json.Unmarshal([]byte(item), task); err != nil {
				glog.Error(err)
				continue
			}
			resp := dl.DownloadUrl(task.Url)
			if resp.Error != nil {
				glog.Error(resp.Error)
				continue
			}
			tasks, records, e := Parse(task.ParserName, resp.Text, task.Url)
			if e != nil {
				glog.Error(e)
				continue
			}
			for _, t := range tasks {
				b, _ := json.Marshal(t)
				if err = crawlTopic.Push(string(b)); err != nil {
					glog.Error(err)
				}
			}
			for _, rec := range records {
				b, _ := json.Marshal(rec)
				if err = storeTopic.Push(string(b)); err != nil {
					glog.Error(err)
				}
			}
			if err = crawlTopic.Confirm(key); err != nil {
				glog.Error(err)
			}
		}
	}
}

func main() {
	flag.Parse()
	defer glog.Flush()

	initTopics()

	exit := make(chan bool)
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go stop(sigs, exit)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go do(i, exit, &wg)
	}
	wg.Wait()

	glog.Info("exit!")
}
