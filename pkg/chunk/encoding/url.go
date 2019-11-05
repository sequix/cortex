package encoding

import (
	"net/url"
	"strings"
)

func RawUrlEncode(s string) string {
	es := url.QueryEscape(s)
	return strings.ReplaceAll(es, "+", "%20")
}

func RawUrlDecode(s string) string {
	rs := strings.ReplaceAll(s, "%20", "+")
	ds, _ := url.QueryUnescape(rs)
	return ds
}
