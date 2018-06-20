package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"crawler.club/dl"
	"crawler.club/et"
	"github.com/golang/glog"
	"github.com/liuzl/store"
	"zliu.org/filestore"
	"zliu.org/goutil"
)

var (
	dir     = flag.String("dir", "data", "working dir")
	timeout = flag.Int64("timeout", 300, "in seconds")
	c       = flag.Int("c", 1, "worker count")
	period  = flag.Int("period", -1, "period in seconds")
	fs      = flag.Bool("fs", true, "filestore flag")
)

var crawlTopic, storeTopic *TaskTopic
var urlStore *store.LevelStore
var fileStore *filestore.FileStore
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
		if *fs {
			fsDir := filepath.Join(*dir, "fs")
			if fileStore, err = filestore.NewFileStore(fsDir); err != nil {
				glog.Error(err)
				return
			}
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
		b, _ := json.Marshal(seed)
		glog.Info(string(b))
		if err = crawlTopic.Push(string(b)); err != nil {
			glog.Error(err)
			return err
		}
	}
	return nil
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

			var t time.Time
			bt, err := urlStore.Get(task.Url)
			if err == nil || err.Error() == "leveldb: not found" {
				t, _ = time.Parse(time.RFC3339, string(bt))
			}

			resp := dl.DownloadUrlWithProxy(task.Url)
			if resp.Error != nil {
				glog.Error(resp.Error)
				continue
			}
			items := strings.Split(resp.RemoteAddr, ":")
			ip := ""
			if len(items) > 0 {
				ip = items[0]
			}
			tasks, records, err := Parse(task.ParserName, resp.Text, task.Url, ip)
			if err != nil {
				glog.Error(err)
				continue
			}

			t2 := time.Now()
			for _, rec := range records {
				b, _ := json.Marshal(rec)
				if *fs {
					fileStore.WriteLine(b)
				}
				glog.Info(string(b))
				if err = storeTopic.Push(string(b)); err != nil {
					glog.Error(err)
				}

				if rec["time_"] != nil {
					switch rec["time_"].(type) {
					case string:
						t2, _ = time.Parse(time.RFC3339, rec["time_"].(string))
					}
				}
			}

			if t2.After(t) {
				for _, t := range tasks {
					b, _ := json.Marshal(t)
					if err = crawlTopic.Push(string(b)); err != nil {
						glog.Error(err)
					}
				}
			}

			if len(tasks) > 0 || len(records) > 0 {
				if err = crawlTopic.Confirm(key); err != nil {
					glog.Error(err)
				}
				urlStore.Put(task.Url, []byte(t2.UTC().Format(time.RFC3339)))
			}
		}
	}
}

func checkSeeds(exit chan bool) {
	defer glog.Info("checkSeeds exit")
	for {
		select {
		case <-exit:
			return
		default:
			goutil.Sleep(time.Duration(*period)*time.Second, exit)
			glog.Info("check seeds period")
			initSeeds()
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
	if *period > 0 && *c > 0 {
		go checkSeeds(exit)
	}
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go do(i, exit, &wg)
	}
	wg.Wait()

	glog.Info("exit!")
}
