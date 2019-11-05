package chunk

import (
	"fmt"
	"strings"
)

// Tags is a string-string map that implements flag.Value.
type Tags map[string]string

// String implements flag.Value
func (ts Tags) String() string {
	if ts == nil {
		return ""
	}

	return fmt.Sprintf("%v", map[string]string(ts))
}

// Set implements flag.Value
func (ts *Tags) Set(s string) error {
	if *ts == nil {
		*ts = map[string]string{}
	}

	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("tag must of the format key=value")
	}
	(*ts)[parts[0]] = parts[1]
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (ts *Tags) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var m map[string]string
	if err := unmarshal(&m); err != nil {
		return err
	}
	*ts = Tags(m)
	return nil
}

// Equals returns true is other matches ts.
func (ts Tags) Equals(other Tags) bool {
	if len(ts) != len(other) {
		return false
	}

	for k, v1 := range ts {
		v2, ok := other[k]
		if !ok || v1 != v2 {
			return false
		}
	}

	return true
}

// Following is a different tag than above one.
//
// Tag represents a high-cardinality KV label.
// It does not participate the computation of the streamID,
// so it will not affect the granularity of the aggregation of chunks.
//
// Tag will store a different index entry, it points to the chunk
// which contains the log line which has the tag.
//
// When performing a query, tag will be used to determine which chunk
// to look at, and it will return a Filter func just like any other Filter.
// And you should use it to filter the lines within that chunk.
type TagMatcher struct {
	Name  string
	Value string
}

func (t *TagMatcher) String() string {
	return fmt.Sprintf("%s=%q", t.Name, t.Value)
}

func (t *TagMatcher) Matches(s string) bool {
	return strings.Contains(s, t.Value)
}

// tagName -> tagValues
type TagMatchers map[string][]string

func (t TagMatchers) Flat() []TagMatcher {
	result := make([]TagMatcher, 0, len(t))
	for n, vs := range t {
		for _, v := range vs {
			result = append(result, TagMatcher{
				Name:  n,
				Value: v,
			})
		}
	}
	return result
}

func (t TagMatchers) AppendString(s string) {
	s = strings.Trim(s, "/")
	KVs := strings.Split(s, ",")
	for _, kv := range KVs {
		kv2 := strings.SplitN(kv, "=", 2)
		if len(kv2) != 2 {
			continue
		}
		tagName := kv2[0]
		tagValue := strings.Trim(kv2[1], `"`)
		t[tagName] = append(t[tagName], tagValue)
	}
}
