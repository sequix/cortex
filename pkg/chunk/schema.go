package chunk

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	chunkTimeRangeKeyV1  = []byte{'1'}
	chunkTimeRangeKeyV2  = []byte{'2'}
	chunkTimeRangeKeyV3  = []byte{'3'}
	chunkTimeRangeKeyV4  = []byte{'4'}
	chunkTimeRangeKeyV5  = []byte{'5'}
	metricNameRangeKeyV1 = []byte{'6'}

	// For v9 schema
	seriesRangeKeyV1      = []byte{'7'}
	labelSeriesRangeKeyV1 = []byte{'8'}
	// For v11 schema
	labelNamesRangeKeyV1 = []byte{'9'}

	// ErrNotSupported when a schema doesn't support that particular lookup.
	ErrNotSupported = errors.New("not supported")
)

// Schema interface defines methods to calculate the hash and range keys needed
// to write or read chunks from the external index.
type Schema interface {
	// When doing a write, use this method to return the list of entries you should write to.
	GetWriteEntries(from, through model.Time, userID string, metricName string, labels labels.Labels, tags TagMatchers, chunkID string) ([]IndexEntry, error)

	// Should only be used with the seriesStore. TODO: Make seriesStore implement a different interface altogether.
	// returns cache key string and []IndexEntry per bucket, matched in order
	GetCacheKeysAndLabelWriteEntries(from, through model.Time, userID string, metricName string, labels labels.Labels, chunkID string) ([]string, [][]IndexEntry, error)
	GetChunkWriteEntries(from, through model.Time, userID string, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error)

	// When doing a read, use these methods to return the list of entries you should query

	// labelNames
	GetReadQueriesForMetric(from, through model.Time, userID string, metricName string) ([]IndexQuery, error)

	// labelName => labelValues
	GetReadQueriesForMetricLabel(from, through model.Time, userID string, metricName string, labelName string) ([]IndexQuery, error)

	// hash(labelName, labelValue) => chunkID
	GetReadQueriesForMetricLabelValue(from, through model.Time, userID string, metricName string, labelName string, labelValue string) ([]IndexQuery, error)

	// tagNames
	GetReadQueriesForTagName(from, through model.Time, userID string) ([]IndexQuery, error)

	// tagName => tagValues
	GetReadQueriesForTagValue(from, through model.Time, userID string, tagName string) ([]IndexQuery, error)

	// hash(tagName,tagValue) => chunkIDs
	GetReadQueriesForTagHash(from, through model.Time, userID string, tagName, tagValue string) ([]IndexQuery, error)

	// hash(tagName, tagValue) => chunkIDs
	GetReadQueriesForChunksFromTagHash(from, through model.Time, userID string, tagName, tagValue string) ([]IndexQuery, error)

	// If the query resulted in series IDs, use this method to find chunks.
	GetChunksForSeries(from, through model.Time, userID string, seriesID []byte) ([]IndexQuery, error)
	// Returns queries to retrieve all label names of multiple series by id.
	GetLabelNamesForSeries(from, through model.Time, userID string, seriesID []byte) ([]IndexQuery, error)
}

// IndexQuery describes a query for entries
type IndexQuery struct {
	TableName string
	HashValue string

	// One of RangeValuePrefix or RangeValueStart might be set:
	// - If RangeValuePrefix is not nil, must read all keys with that prefix.
	// - If RangeValueStart is not nil, must read all keys from there onwards.
	// - If neither is set, must read all keys for that row.
	RangeValuePrefix []byte
	RangeValueStart  []byte

	// Filters for querying
	ValueEqual []byte

	// If the result of this lookup is immutable or not (for caching).
	Immutable bool
}

func (iq IndexQuery) String() string {
	return iq.TableName + ":" + iq.HashValue + ":" + string(iq.RangeValuePrefix)
}

// IndexEntry describes an entry in the chunk index
type IndexEntry struct {
	TableName string
	HashValue string

	// For writes, RangeValue will always be set.
	RangeValue []byte

	// New for v6 schema, label value is not written as part of the range key.
	Value []byte
}

type schemaBucketsFunc func(from, through model.Time, userID string) []Bucket

// schema implements Schema given a bucketing function and and set of range key callbacks
type schema struct {
	buckets schemaBucketsFunc
	entries entries
}

func (s schema) GetWriteEntries(from, through model.Time, userID string, metricName string, labels labels.Labels, tags TagMatchers, chunkID string) ([]IndexEntry, error) {
	var result []IndexEntry

	for _, bucket := range s.buckets(from, through, userID) {
		entries, err := s.entries.GetWriteEntries(bucket, metricName, labels, tags, chunkID)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

// returns cache key string and []IndexEntry per bucket, matched in order
func (s schema) GetCacheKeysAndLabelWriteEntries(from, through model.Time, userID string, metricName string, labels labels.Labels, chunkID string) ([]string, [][]IndexEntry, error) {
	var keys []string
	var indexEntries [][]IndexEntry

	for _, bucket := range s.buckets(from, through, userID) {
		key := strings.Join([]string{
			bucket.tableName,
			bucket.hashKey,
			string(labelsSeriesID(labels)),
		},
			"-",
		)
		// This is just encoding to remove invalid characters so that we can put them in memcache.
		// We're not hashing them as the length of the key is well within memcache bounds. tableName + userid + day + 32Byte(seriesID)
		key = hex.EncodeToString([]byte(key))
		keys = append(keys, key)

		entries, err := s.entries.GetLabelWriteEntries(bucket, metricName, labels, chunkID)
		if err != nil {
			return nil, nil, err
		}
		indexEntries = append(indexEntries, entries)
	}
	return keys, indexEntries, nil
}

func (s schema) GetChunkWriteEntries(from, through model.Time, userID string, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	var result []IndexEntry

	for _, bucket := range s.buckets(from, through, userID) {
		entries, err := s.entries.GetChunkWriteEntries(bucket, metricName, labels, chunkID)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil

}

func (s schema) GetReadQueriesForMetric(from, through model.Time, userID string, metricName string) ([]IndexQuery, error) {
	var result []IndexQuery

	buckets := s.buckets(from, through, userID)
	for _, bucket := range buckets {
		entries, err := s.entries.GetReadMetricQueries(bucket, metricName)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetReadQueriesForMetricLabel(from, through model.Time, userID string, metricName string, labelName string) ([]IndexQuery, error) {
	var result []IndexQuery

	buckets := s.buckets(from, through, userID)
	for _, bucket := range buckets {
		entries, err := s.entries.GetReadMetricLabelQueries(bucket, metricName, labelName)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetReadQueriesForMetricLabelValue(from, through model.Time, userID string, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	var result []IndexQuery

	buckets := s.buckets(from, through, userID)
	for _, bucket := range buckets {
		entries, err := s.entries.GetReadMetricLabelValueQueries(bucket, metricName, labelName, labelValue)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetChunksForSeries(from, through model.Time, userID string, seriesID []byte) ([]IndexQuery, error) {
	var result []IndexQuery

	buckets := s.buckets(from, through, userID)
	for _, bucket := range buckets {
		entries, err := s.entries.GetChunksForSeries(bucket, seriesID)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetLabelNamesForSeries(from, through model.Time, userID string, seriesID []byte) ([]IndexQuery, error) {
	var result []IndexQuery

	buckets := s.buckets(from, through, userID)
	for _, bucket := range buckets {
		entries, err := s.entries.GetLabelNamesForSeries(bucket, seriesID)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetReadQueriesForTagName(from, through model.Time, userID string) ([]IndexQuery, error) {
	var result []IndexQuery
	for _, bucket := range s.buckets(from, through, userID) {
		entries, err := s.entries.GetReadMetricTagNameQueries(bucket)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetReadQueriesForTagValue(from, through model.Time, userID string, tagName string) ([]IndexQuery, error) {
	var result []IndexQuery
	for _, bucket := range s.buckets(from, through, userID) {
		entries, err := s.entries.GetReadMetricTagValueQueries(bucket, tagName)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetReadQueriesForTagHash(from, through model.Time, userID string, tagName, tagValue string) ([]IndexQuery, error) {
	var result []IndexQuery
	for _, bucket := range s.buckets(from, through, userID) {
		entries, err := s.entries.GetReadMetricTagHashQueries(bucket, tagName, tagValue)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s schema) GetReadQueriesForChunksFromTagHash(from, through model.Time, userID string, tagName, tagValue string) ([]IndexQuery, error) {
	panic("implement me")
}

type entries interface {
	// Loki schema中只使用该接口，用于生成所有的entries
	GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, tags TagMatchers, chunkID string) ([]IndexEntry, error)
	GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error)
	GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error)

	// get all label names
	GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error)

	// get all label values under specific labelName
	GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error)

	// get all streamIDs by specific label
	GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error)

	// get all tag names
	GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error)

	// get all tag values under specific tagName
	GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error)

	// get all chunkIDs by specific tag
	GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error)

	GetChunksForSeries(bucket Bucket, seriesID []byte) ([]IndexQuery, error)
	GetLabelNamesForSeries(bucket Bucket, seriesID []byte) ([]IndexQuery, error)
}

// original entries:
// - hash key: <userid>:<bucket>:<metric name>
// - range key: <label name>\0<label value>\0<chunk name>

type originalEntries struct{}

func (originalEntries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (originalEntries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (originalEntries) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (originalEntries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	result := []IndexEntry{}
	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		if strings.ContainsRune(string(v.Value), '\x00') {
			return nil, fmt.Errorf("label values cannot contain null byte")
		}
		result = append(result, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + metricName,
			RangeValue: encodeRangeKey([]byte(v.Name), []byte(v.Value), chunkIDBytes),
		})
	}
	return result, nil
}

func (originalEntries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}
func (originalEntries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (originalEntries) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName:        bucket.tableName,
			HashValue:        bucket.hashKey + ":" + metricName,
			RangeValuePrefix: nil,
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName:        bucket.tableName,
			HashValue:        bucket.hashKey + ":" + metricName,
			RangeValuePrefix: encodeRangeKey([]byte(labelName)),
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	if strings.ContainsRune(string(labelValue), '\x00') {
		return nil, fmt.Errorf("label values cannot contain null byte")
	}
	return []IndexQuery{
		{
			TableName:        bucket.tableName,
			HashValue:        bucket.hashKey + ":" + metricName,
			RangeValuePrefix: encodeRangeKey([]byte(labelName), []byte(labelValue)),
		},
	}, nil
}

func (originalEntries) GetChunksForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (originalEntries) GetLabelNamesForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

// v3Schema went to base64 encoded label values & a version ID
// - range key: <label name>\0<base64(label value)>\0<chunk name>\0<version 1>

type base64Entries struct {
	originalEntries
}

func (base64Entries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (base64Entries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (base64Entries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	result := []IndexEntry{}
	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}

		encodedBytes := encodeBase64Value(v.Value)
		result = append(result, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + metricName,
			RangeValue: encodeRangeKey([]byte(v.Name), encodedBytes, chunkIDBytes, chunkTimeRangeKeyV1),
		})
	}
	return result, nil
}

func (base64Entries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}
func (base64Entries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (base64Entries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	encodedBytes := encodeBase64Value(labelValue)
	return []IndexQuery{
		{
			TableName:        bucket.tableName,
			HashValue:        bucket.hashKey + ":" + metricName,
			RangeValuePrefix: encodeRangeKey([]byte(labelName), encodedBytes),
		},
	}, nil
}

// v4 schema went to two schemas in one:
// 1) - hash key: <userid>:<hour bucket>:<metric name>:<label name>
//    - range key: \0<base64(label value)>\0<chunk name>\0<version 2>
// 2) - hash key: <userid>:<hour bucket>:<metric name>
//    - range key: \0\0<chunk name>\0<version 3>
type labelNameInHashKeyEntries struct{}

func (labelNameInHashKeyEntries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (labelNameInHashKeyEntries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (labelNameInHashKeyEntries) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (labelNameInHashKeyEntries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	entries := []IndexEntry{
		{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + metricName,
			RangeValue: encodeRangeKey(nil, nil, chunkIDBytes, chunkTimeRangeKeyV2),
		},
	}

	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		encodedBytes := encodeBase64Value(v.Value)
		entries = append(entries, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, v.Name),
			RangeValue: encodeRangeKey(nil, encodedBytes, chunkIDBytes, chunkTimeRangeKeyV1),
		})
	}

	return entries, nil
}

func (labelNameInHashKeyEntries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}
func (labelNameInHashKeyEntries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (labelNameInHashKeyEntries) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: bucket.hashKey + ":" + metricName,
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	encodedBytes := encodeBase64Value(labelValue)
	return []IndexQuery{
		{
			TableName:        bucket.tableName,
			HashValue:        fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
			RangeValuePrefix: encodeRangeKey(nil, encodedBytes),
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetChunksForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (labelNameInHashKeyEntries) GetLabelNamesForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

// v5 schema is an extension of v4, with the chunk end time in the
// range key to improve query latency.  However, it did it wrong
// so the chunk end times are ignored.
type v5Entries struct{}

func (v5Entries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v5Entries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v5Entries) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v5Entries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(bucket.through)

	entries := []IndexEntry{
		{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + metricName,
			RangeValue: encodeRangeKey(encodedThroughBytes, nil, chunkIDBytes, chunkTimeRangeKeyV3),
		},
	}

	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		encodedValueBytes := encodeBase64Value(v.Value)
		entries = append(entries, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, v.Name),
			RangeValue: encodeRangeKey(encodedThroughBytes, encodedValueBytes, chunkIDBytes, chunkTimeRangeKeyV4),
		})
	}

	return entries, nil
}

func (v5Entries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}
func (v5Entries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (v5Entries) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: bucket.hashKey + ":" + metricName,
		},
	}, nil
}

func (v5Entries) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
		},
	}, nil
}

func (v5Entries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, _ string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
		},
	}, nil
}

func (v5Entries) GetChunksForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v5Entries) GetLabelNamesForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

// v6Entries fixes issues with v5 time encoding being wrong (see #337), and
// moves label value out of range key (see #199).
type v6Entries struct{}

func (v6Entries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v6Entries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v6Entries) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v6Entries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(bucket.through)

	entries := []IndexEntry{
		{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + metricName,
			RangeValue: encodeRangeKey(encodedThroughBytes, nil, chunkIDBytes, chunkTimeRangeKeyV3),
		},
	}

	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		entries = append(entries, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, v.Name),
			RangeValue: encodeRangeKey(encodedThroughBytes, nil, chunkIDBytes, chunkTimeRangeKeyV5),
			Value:      []byte(v.Value),
		})
	}

	return entries, nil
}

func (v6Entries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}
func (v6Entries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (v6Entries) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	encodedFromBytes := encodeTime(bucket.from)
	return []IndexQuery{
		{
			TableName:       bucket.tableName,
			HashValue:       bucket.hashKey + ":" + metricName,
			RangeValueStart: encodeRangeKey(encodedFromBytes),
		},
	}, nil
}

func (v6Entries) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	encodedFromBytes := encodeTime(bucket.from)
	return []IndexQuery{
		{
			TableName:       bucket.tableName,
			HashValue:       fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
			RangeValueStart: encodeRangeKey(encodedFromBytes),
		},
	}, nil
}

func (v6Entries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	encodedFromBytes := encodeTime(bucket.from)
	return []IndexQuery{
		{
			TableName:       bucket.tableName,
			HashValue:       fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
			RangeValueStart: encodeRangeKey(encodedFromBytes),
			ValueEqual:      []byte(labelValue),
		},
	}, nil
}

func (v6Entries) GetChunksForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v6Entries) GetLabelNamesForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

// v9Entries adds a layer of indirection between labels -> series -> chunks.
type v9Entries struct{}

func (v9Entries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v9Entries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v9Entries) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v9Entries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (v9Entries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	seriesID := labelsSeriesID(labels)

	entries := []IndexEntry{
		// Entry for metricName -> seriesID
		{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + metricName,
			RangeValue: encodeRangeKey(seriesID, nil, nil, seriesRangeKeyV1),
		},
	}

	// Entries for metricName:labelName -> hash(value):seriesID
	// We use a hash of the value to limit its length.
	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		valueHash := sha256bytes(v.Value)
		entries = append(entries, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, v.Name),
			RangeValue: encodeRangeKey(valueHash, seriesID, nil, labelSeriesRangeKeyV1),
			Value:      []byte(v.Value),
		})
	}

	return entries, nil
}

func (v9Entries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	seriesID := labelsSeriesID(labels)
	encodedThroughBytes := encodeTime(bucket.through)

	entries := []IndexEntry{
		// Entry for seriesID -> chunkID
		{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + string(seriesID),
			RangeValue: encodeRangeKey(encodedThroughBytes, nil, []byte(chunkID), chunkTimeRangeKeyV3),
		},
	}

	return entries, nil
}

func (v9Entries) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: bucket.hashKey + ":" + metricName,
		},
	}, nil
}

func (v9Entries) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
		},
	}, nil
}

func (v9Entries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	valueHash := sha256bytes(labelValue)
	return []IndexQuery{
		{
			TableName:       bucket.tableName,
			HashValue:       fmt.Sprintf("%s:%s:%s", bucket.hashKey, metricName, labelName),
			RangeValueStart: encodeRangeKey(valueHash),
			ValueEqual:      []byte(labelValue),
		},
	}, nil
}

func (v9Entries) GetChunksForSeries(bucket Bucket, seriesID []byte) ([]IndexQuery, error) {
	encodedFromBytes := encodeTime(bucket.from)
	return []IndexQuery{
		{
			TableName:       bucket.tableName,
			HashValue:       bucket.hashKey + ":" + string(seriesID),
			RangeValueStart: encodeRangeKey(encodedFromBytes),
		},
	}, nil
}

func (v9Entries) GetLabelNamesForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

// v10Entries builds on v9 by sharding index rows to reduce their size.
type v10Entries struct {
	rowShards uint32
}

func (v10Entries) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v10Entries) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v10Entries) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

func (v10Entries) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, _ TagMatchers, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (s v10Entries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	seriesID := labelsSeriesID(labels)

	// read first 32 bits of the hash and use this to calculate the shard
	shard := binary.BigEndian.Uint32(seriesID) % s.rowShards

	entries := []IndexEntry{
		// Entry for metricName -> seriesID
		{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%02d:%s:%s", shard, bucket.hashKey, metricName),
			RangeValue: encodeRangeKey(seriesID, nil, nil, seriesRangeKeyV1),
		},
	}

	// Entries for metricName:labelName -> hash(value):seriesID
	// We use a hash of the value to limit its length.
	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		valueHash := sha256bytes(v.Value)
		entries = append(entries, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%02d:%s:%s:%s", shard, bucket.hashKey, metricName, v.Name),
			RangeValue: encodeRangeKey(valueHash, seriesID, nil, labelSeriesRangeKeyV1),
			Value:      []byte(v.Value),
		})
	}

	return entries, nil
}

func (v10Entries) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	seriesID := labelsSeriesID(labels)
	encodedThroughBytes := encodeTime(bucket.through)

	entries := []IndexEntry{
		// Entry for seriesID -> chunkID
		{
			TableName:  bucket.tableName,
			HashValue:  bucket.hashKey + ":" + string(seriesID),
			RangeValue: encodeRangeKey(encodedThroughBytes, nil, []byte(chunkID), chunkTimeRangeKeyV3),
		},
	}

	return entries, nil
}

func (s v10Entries) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	result := make([]IndexQuery, 0, s.rowShards)
	for i := uint32(0); i < s.rowShards; i++ {
		result = append(result, IndexQuery{
			TableName: bucket.tableName,
			HashValue: fmt.Sprintf("%02d:%s:%s", i, bucket.hashKey, metricName),
		})
	}
	return result, nil
}

func (s v10Entries) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	result := make([]IndexQuery, 0, s.rowShards)
	for i := uint32(0); i < s.rowShards; i++ {
		result = append(result, IndexQuery{
			TableName: bucket.tableName,
			HashValue: fmt.Sprintf("%02d:%s:%s:%s", i, bucket.hashKey, metricName, labelName),
		})
	}
	return result, nil
}

func (s v10Entries) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	valueHash := sha256bytes(labelValue)
	result := make([]IndexQuery, 0, s.rowShards)
	for i := uint32(0); i < s.rowShards; i++ {
		result = append(result, IndexQuery{
			TableName:       bucket.tableName,
			HashValue:       fmt.Sprintf("%02d:%s:%s:%s", i, bucket.hashKey, metricName, labelName),
			RangeValueStart: encodeRangeKey(valueHash),
			ValueEqual:      []byte(labelValue),
		})
	}
	return result, nil
}

func (v10Entries) GetChunksForSeries(bucket Bucket, seriesID []byte) ([]IndexQuery, error) {
	encodedFromBytes := encodeTime(bucket.from)
	return []IndexQuery{
		{
			TableName:       bucket.tableName,
			HashValue:       bucket.hashKey + ":" + string(seriesID),
			RangeValueStart: encodeRangeKey(encodedFromBytes),
		},
	}, nil
}

func (v10Entries) GetLabelNamesForSeries(_ Bucket, _ []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

// v11Entries builds on v10 but adds index entries for each series to store respective labels.
type v11Entries struct {
	v10Entries
}

func (s v11Entries) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	seriesID := labelsSeriesID(labels)

	// read first 32 bits of the hash and use this to calculate the shard
	shard := binary.BigEndian.Uint32(seriesID) % s.rowShards

	labelNames := make([]string, 0, len(labels))
	for _, l := range labels {
		if l.Name == model.MetricNameLabel {
			continue
		}
		labelNames = append(labelNames, l.Name)
	}
	data, err := jsoniter.ConfigFastest.Marshal(labelNames)
	if err != nil {
		return nil, err
	}
	entries := []IndexEntry{
		// Entry for metricName -> seriesID
		{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%02d:%s:%s", shard, bucket.hashKey, metricName),
			RangeValue: encodeRangeKey(seriesID, nil, nil, seriesRangeKeyV1),
		},
		// Entry for seriesID -> label names
		{
			TableName:  bucket.tableName,
			HashValue:  string(seriesID),
			RangeValue: encodeRangeKey(nil, nil, nil, labelNamesRangeKeyV1),
			Value:      data,
		},
	}

	// Entries for metricName:labelName -> hash(value):seriesID
	// We use a hash of the value to limit its length.
	for _, v := range labels {
		if v.Name == model.MetricNameLabel {
			continue
		}
		valueHash := sha256bytes(v.Value)
		entries = append(entries, IndexEntry{
			TableName:  bucket.tableName,
			HashValue:  fmt.Sprintf("%02d:%s:%s:%s", shard, bucket.hashKey, metricName, v.Name),
			RangeValue: encodeRangeKey(valueHash, seriesID, nil, labelSeriesRangeKeyV1),
			Value:      []byte(v.Value),
		})
	}

	return entries, nil
}

func (v11Entries) GetLabelNamesForSeries(bucket Bucket, seriesID []byte) ([]IndexQuery, error) {
	return []IndexQuery{
		{
			TableName: bucket.tableName,
			HashValue: string(seriesID),
		},
	}, nil
}

// 除chunkID外所有的字段都是固定长度的，其目的在于只使用一个排序键就能完成所有的查询
//
// HashKey: <version>:<userID>:<timeRange>:L
// RangeKey: <labelName>
//
// HashKey: <version>:<userID>:<timeRange>:N:<labelName>
// RangeKey: <labelValue>
//
// HashKey: <version>:<userID>:<timeRange>:V:<hash(labelName,labelValue)>
// RangeKey: <streamID>\x00<chunkID>
//
// TODO 给chunkID索引加上时间范围
// HashKey: <version>:<userID>:<timeRange>:S:<streamID>
// RangeKey: <chunkID>
//
// HashKey: <version>:<userID>:<timeRange>:T
// RangeKey: <tagName>
//
// HashKey: <version>:<userID>:<timeRange>:Z:<shard>:<tagName>
// RangeKey: <tagValue>
//
// HashKey: <version>:<userID>:<timeRange>:H:<hash(tagName,tagValue)>
// RangeKey: <streamID>
type lokiEntriesV1 struct {
	rowShards uint64
}

func (lokiEntriesV1) GetLabelNamesForSeries(bucket Bucket, seriesID []byte) ([]IndexQuery, error) {
	return nil, ErrNotSupported
}

const (
	LokiSeparator     = "\x00"
	LokiSeparatorNext = "\x01"
)

const (
	LokiEntryKindLabelNames               = "L"
	LokiEntryKindLabelName2Values         = "N"
	LokiEntryKindLabelHash2StreamChunkIDs = "V"
	LokiEntryKindStreamID2ChunkIDs        = "S"
	LokiEntryKindTagNames                 = "T"
	LokiEntryKindTagName2Values           = "Z"
	LokiEntryKindTagHash2ChunkIDs         = "H"
)

func (e lokiEntriesV1) GetWriteEntries(bucket Bucket, metricName string, labels labels.Labels, tags TagMatchers, chunkID string) ([]IndexEntry, error) {
	var (
		chunkIDBytes = []byte(chunkID)
		streamID     = labelsSeriesID(labels)
		entries      = make([]IndexEntry, 0, 1+3*(len(labels)+len(tags)))
	)

	// HashKey: <version>:<userID>:<timeRange>:S:<streamID>
	// RangeKey: <chunkID>
	entries = append(entries, IndexEntry{
		HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindStreamID2ChunkIDs + ":" + string(streamID),
		RangeValue: []byte(chunkID),
	})

	for i := range labels {
		lb := &labels[i]

		if lb.Name == model.MetricNameLabel {
			continue
		}
		_, lbHash := xxhashString(lb.Name + LokiSeparator + lb.Value)

		// HashKey: <version>:<userID>:<timeRange>:L
		// RangeKey: <labelName>
		entries = append(entries, IndexEntry{
			HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindLabelNames,
			RangeValue: []byte(lb.Name),
		})

		// HashKey: <version>:<userID>:<timeRange>:N:<labelName>
		// RangeKey: <labelValue>
		entries = append(entries, IndexEntry{
			HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindLabelName2Values + ":" + lb.Name,
			RangeValue: []byte(lb.Value),
		})

		// HashKey: <version>:<userID>:<timeRange>:V:<hash(labelName,labelValue)>
		// RangeKey: <streamID>
		entries = append(entries, IndexEntry{
			HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindLabelHash2StreamChunkIDs + ":" + lbHash,
			RangeValue: encodeRangeKey(streamID, chunkIDBytes),
		})
	}

	for tagName, tagValues := range tags {
		// HashKey: <version>:<userID>:<timeRange>:T
		// RangeKey: <tagName>
		entries = append(entries, IndexEntry{
			HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindTagNames,
			RangeValue: []byte(tagName),
		})

		for _, tagValue := range tagValues {
			tagHashInt, tagHash := xxhashString(tagName + LokiSeparator + tagValue)
			shard := fmt.Sprintf("%02d", tagHashInt%e.rowShards)

			// HashKey: <version>:<userID>:<timeRange>:Z:<shard>:<tagName>
			// RangeKey: <tagValue>
			entries = append(entries, IndexEntry{
				HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindTagName2Values + ":" + shard + ":" + tagName,
				RangeValue: []byte(tagValue),
			})

			// HashKey: <version>:<userID>:<timeRange>:H:<hash(tagName,tagValue)>
			// RangeKey: <chunkID>
			entries = append(entries, IndexEntry{
				HashValue:  "1:" + bucket.hashKey + ":" + LokiEntryKindTagHash2ChunkIDs + ":" + tagHash,
				RangeValue: chunkIDBytes,
			})

		}
	}
	return entries, nil
}

func (lokiEntriesV1) GetLabelWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

func (lokiEntriesV1) GetChunkWriteEntries(bucket Bucket, metricName string, labels labels.Labels, chunkID string) ([]IndexEntry, error) {
	return nil, ErrNotSupported
}

// get all label names
func (lokiEntriesV1) GetReadMetricQueries(bucket Bucket, metricName string) ([]IndexQuery, error) {
	queries := []IndexQuery{
		{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindLabelNames,
		},
	}
	return queries, nil
}

// get all label values under specific labelName
func (e lokiEntriesV1) GetReadMetricLabelQueries(bucket Bucket, metricName string, labelName string) ([]IndexQuery, error) {
	queries := []IndexQuery{
		{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindLabelName2Values + ":" + labelName,
		},
	}
	return queries, nil
}

// get all streamIDs by specific label
func (lokiEntriesV1) GetReadMetricLabelValueQueries(bucket Bucket, metricName string, labelName string, labelValue string) ([]IndexQuery, error) {
	_, labelHash := xxhashString(labelName + LokiSeparator + labelValue)
	queries := []IndexQuery{
		{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindLabelHash2StreamChunkIDs + ":" + labelHash,
		},
	}
	return queries, nil
}

// get all tag names
func (e lokiEntriesV1) GetReadMetricTagNameQueries(bucket Bucket) ([]IndexQuery, error) {
	queries := []IndexQuery{
		{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindTagNames,
		},
	}
	return queries, nil
}

// get all tag values under specific tagName
func (e lokiEntriesV1) GetReadMetricTagValueQueries(bucket Bucket, tagName string) ([]IndexQuery, error) {
	queries := make([]IndexQuery, 0, e.rowShards)
	for i := uint64(0); i < e.rowShards; i++ {
		shard := fmt.Sprintf("%02d", i)
		queries = append(queries, IndexQuery{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindTagName2Values + ":" + shard + ":" + tagName,
		})
	}
	return queries, nil
}

// get all tag values under specific tagName
func (lokiEntriesV1) GetReadMetricTagHashQueries(bucket Bucket, tagName, tagValue string) ([]IndexQuery, error) {
	_, tagHash := xxhashString(tagName + LokiSeparator + tagValue)
	queries := []IndexQuery{
		{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindTagHash2ChunkIDs + ":" + tagHash,
		},
	}
	return queries, nil
}

func (lokiEntriesV1) GetChunksForSeries(bucket Bucket, streamID []byte) ([]IndexQuery, error) {
	queries := []IndexQuery{
		{
			HashValue: "1:" + bucket.hashKey + ":" + LokiEntryKindStreamID2ChunkIDs + ":" + string(streamID),
		},
	}
	return queries, nil
}
