package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/liuzl/ds"
	"github.com/liuzl/goutil"
	"github.com/liuzl/store"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var debug = false

type Queue struct {
	Name string `json:"name"`
	Dir  string `json:dir`

	queue        *ds.Queue
	retryQueue   *ds.Queue
	runningStore *store.LevelStore
	exit         chan bool
}

func NewQueue(name, dir string) (*Queue, error) {
	t := &Queue{Name: name, Dir: dir, exit: make(chan bool)}
	var err error
	queueDir := filepath.Join(dir, name, "queue")
	if t.queue, err = ds.OpenQueue(queueDir); err != nil {
		return nil, err
	}
	retryDir := filepath.Join(dir, name, "retry_queue")
	if t.retryQueue, err = ds.OpenQueue(retryDir); err != nil {
		return nil, err
	}
	storeDir := filepath.Join(dir, name, "running")
	if t.runningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}

	go t.retry()

	return t, nil
}

func (t *Queue) Enqueue(data string) error {
	if debug {
		fmt.Println("Enqueue:", data, t.queue.Length(), t.retryQueue.Length())
	}
	if t.queue != nil {
		_, err := t.queue.EnqueueString(data)
		return err
	}
	return fmt.Errorf("queue is nil")
}

func (t *Queue) dequeue(q *ds.Queue, timeout int64) (string, string, error) {
	item, err := q.Dequeue()
	if err != nil {
		return "", "", err
	}
	key := ""
	if timeout > 0 {
		now := time.Now().Unix()
		key = goutil.TimeStr(now+timeout) + ":" + goutil.ContentMD5(item.Value)
		if err = t.addToRunning(key, item.Value); err != nil {
			return "", "", err
		}
	}
	return key, string(item.Value), nil
}

func (t *Queue) Dequeue(timeout int64) (string, string, error) {
	if t.retryQueue != nil && t.retryQueue.Length() > 0 {
		return t.dequeue(t.retryQueue, timeout)
	}
	if t.queue != nil && t.queue.Length() > 0 {
		return t.dequeue(t.queue, timeout)
	}
	return "", "", fmt.Errorf("Queue is empty")
}

func (t *Queue) Confirm(key string) error {
	if debug {
		fmt.Println("confirm", key, t.queue.Length(), t.retryQueue.Length())
	}
	if t.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return t.runningStore.Delete(key)
}

func (t *Queue) Close() {
	if t.exit != nil {
		t.exit <- true
	}
	if t.queue != nil {
		t.queue.Close()
	}
	if t.retryQueue != nil {
		t.retryQueue.Close()
	}
	if t.runningStore != nil {
		t.runningStore.Close()
	}
}

func (t *Queue) Drop() {
	t.Close()
	path := filepath.Join(t.Dir, t.Name)
	os.RemoveAll(path)
}

func (t *Queue) addToRunning(key string, value []byte) error {
	if debug {
		fmt.Println("addToRunning", key, t.queue.Length(), t.retryQueue.Length())
	}
	if len(value) == 0 {
		return fmt.Errorf("empty value")
	}
	if t.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return t.runningStore.Put(key, value)
}

func (t *Queue) retry() {
	for {
		select {
		case <-t.exit:
			return
		default:
			now := time.Now().Format("20060102030405")
			t.runningStore.ForEach(&util.Range{Limit: []byte(now)},
				func(key, value []byte) (bool, error) {
					if _, err := t.retryQueue.Enqueue(value); err != nil {
						return false, err
					}
					t.runningStore.Delete(string(key))
					return true, nil
				})
			goutil.Sleep(5*time.Second, t.exit)
		}
	}
}
