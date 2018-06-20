package main

import (
	"fmt"

	"crawler.club/et"
	"zliu.org/goutil"
)

func taskKey(t *et.UrlTask) string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("%s\t%s\t%s",
		goutil.ReverseOrderEncode(t.TaskName), t.Url, t.ParserName)
}
