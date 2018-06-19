package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"crawler.club/dl"
	"crawler.club/et"
	"github.com/golang/glog"
	"github.com/liuzl/store"
	"zliu.org/goutil"
)

var (
	dir     = flag.String("dir", "data", "working dir")
	timeout = flag.Int64("timeout", 300, "in seconds")
)

var crawlTopic, storeTopic *TaskTopic
var urlStore *store.LevelStore
var once sync.Once

func initTopics() (err error) {
	once.Do(func() {
		if crawlTopic, err = NewTaskTopic("crawl", *dir); err != nil {
			glog.Error(err)
			return
		}
		if storeTopic, err = NewTaskTopic("store", *dir); err != nil {
			glog.Error(err)
			return
		}
		dbDir := filepath.Join(*dir, "url")
		if urlStore, err = store.NewLevelStore(dbDir); err != nil {
			glog.Error(err)
			return
		}
		if err = initSeeds(); err != nil {
			return
		}
	})
	return
}

func initSeeds() error {
	seedsFile := filepath.Join(*conf, "seeds.json")
	content, err := ioutil.ReadFile(seedsFile)
	if err != nil {
		glog.Error(err)
		return err
	}
	var seeds []*et.UrlTask
	if err = json.Unmarshal(content, &seeds); err != nil {
		glog.Error(err)
		return err
	}
	for _, seed := range seeds {
		if check(seed) {
			continue
		}
		b, _ := json.Marshal(seed)
		glog.Info(string(b))
		if err = crawlTopic.Push(string(b)); err != nil {
			glog.Error(err)
			return err
		}
	}
	return nil
}

func check(task *et.UrlTask) bool {
	if task == nil || task.Url == "" || task.ParserName == "" {
		return true
	}
	return false
}

func stop(sigs chan os.Signal, exit chan bool) {
	<-sigs
	glog.Info("receive stop signal!")
	close(exit)
}

func do(i int, exit chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	glog.Infof("start worker %d", i)
	num := 0
	for {
		select {
		case <-exit:
			glog.Infof("worker %d exit", i)
			return
		default:
			num++
			key, item, err := crawlTopic.Pop(*timeout)
			glog.Infof("%d: err=%+v", num, err)
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
			resp := dl.DownloadUrlWithProxy(task.Url)
			if resp.Error != nil {
				glog.Error(resp.Error)
				continue
			}
			tasks, records, err := Parse(task.ParserName, resp.Text, task.Url)
			if err != nil {
				glog.Error(err)
				continue
			}
			for _, t := range tasks {
				if check(t) {
					continue
				}

				b, _ := json.Marshal(t)
				if err = crawlTopic.Push(string(b)); err != nil {
					glog.Error(err)
				}
			}
			for _, rec := range records {
				b, _ := json.Marshal(rec)
				glog.Info(string(b))
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

	if err := initTopics(); err != nil {
		return
	}

	exit := make(chan bool)
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go stop(sigs, exit)

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go do(i, exit, &wg)
	}
	wg.Wait()

	glog.Info("exit!")
}
