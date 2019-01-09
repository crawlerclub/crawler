package rss

import (
	"github.com/xgolib/gofeed"
)

func Parse(url, page string, ext interface{}) ([]map[string]interface{}, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseString(page)
	if err != nil {
		return nil, err
	}
	var ret []map[string]interface{}
	for _, item := range feed.Items {
		ret = append(ret, map[string]interface{}{"feed": item, "ext": ext})
	}
	return ret, nil
}
