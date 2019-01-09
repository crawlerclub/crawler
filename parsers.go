package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"

	"crawler.club/crawler/rss"
	"crawler.club/et"
	"github.com/crawlerclub/ce"
)

var (
	conf = flag.String("conf", "./conf", "dir for parsers conf")
)

type Parsers struct {
	sync.Mutex
	items map[string]*et.Parser
}

func (p *Parsers) GetParser(name string, refresh bool) (*et.Parser, error) {
	p.Lock()
	defer p.Unlock()
	if !refresh && p.items[name] != nil {
		return p.items[name], nil
	}
	file := filepath.Join(*conf, "parsers", name+".json")
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	parser := new(et.Parser)
	if err := json.Unmarshal(content, parser); err != nil {
		return nil, err
	}
	p.items[name] = parser
	return parser, nil
}

var pool = &Parsers{items: make(map[string]*et.Parser)}

func Parse(task *et.UrlTask, page, ip string) (
	[]*et.UrlTask, []map[string]interface{}, error) {
	name := task.ParserName
	url := task.Url
	switch strings.ToLower(name) {
	case "rss_":
		feeds, err := rss.Parse(url, page, task.Ext)
		return nil, feeds, err
	case "content_":
		doc := ce.ParsePro(url, page, ip, false)
		return nil, []map[string]interface{}{map[string]interface{}{"doc": doc, "ext": task.Ext}}, nil
	case "link_":
		links, err := et.ParseNewLinks(page, url)
		if err != nil {
			return nil, nil, err
		}
		var tasks []*et.UrlTask
		for _, link := range links {
			tasks = append(tasks, &et.UrlTask{ParserName: "content_", Url: link, Ext: task.Ext})
		}
		return tasks, nil, nil
	default:
		p, err := pool.GetParser(name, false)
		if err != nil {
			return nil, nil, err
		}
		return p.Parse(page, url)
	}
}
