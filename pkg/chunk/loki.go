package chunk

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// TODO index cache
// TODO metric

type lokiStore struct {
	store
}

func newLokiStore(cfg StoreConfig, schema Schema, index IndexClient, chunks ObjectClient, limits StoreLimits) (Store, error) {
	fetcher, err := NewChunkFetcher(cfg.ChunkCacheConfig, cfg.chunkCacheStubs, chunks)
	if err != nil {
		return nil, err
	}

	return &lokiStore{
		store: store{
			cfg:     cfg,
			index:   index,
			chunks:  chunks,
			schema:  schema,
			limits:  limits,
			Fetcher: fetcher,
		},
	}, nil
}

func (ls lokiStore) Get(ctx context.Context, userID string, from, through model.Time, matchers []*labels.Matcher, tags []*TagMatcher) ([]Chunk, error) {
	log, ctx := spanlogger.New(ctx, "LokiChunkStore.Get")
	defer log.Span.Finish()
	level.Debug(log).Log("from", from, "through", through, "matchers", len(matchers))

	// Validate the query is within reasonable bounds.
	_, matchers, shortcut, err := ls.validateQuery(ctx, userID, &from, &through, matchers)
	if err != nil {
		return nil, err
	} else if shortcut {
		return nil, nil
	}
	return ls.getLokiChunks(ctx, userID, from, through, matchers, tags)
}

func (ls lokiStore) getLokiChunks(ctx context.Context, userID string, from, through model.Time, matchers []*labels.Matcher, tags []*TagMatcher) ([]Chunk, error) {
	log, ctx := spanlogger.New(ctx, "LokiChunkStore.getLokiChunks")
	defer log.Finish()
	level.Debug(log).Log("from", from, "through", through, "matchers", len(matchers), "tags", len(tags))

	chunks, err := ls.lookupChunkIDsByMatchersAndTags(ctx, userID, from, through, matchers, tags)
	if err != nil {
		return nil, err
	}

	// Filter out chunks that are not in the selected time range.
	filtered := filterChunksByTime(from, through, chunks)
	level.Debug(log).Log("Chunks post filtering", len(chunks))

	maxChunksPerQuery := ls.limits.MaxChunksPerQuery(userID)
	if maxChunksPerQuery > 0 && len(filtered) > maxChunksPerQuery {
		err := httpgrpc.Errorf(http.StatusBadRequest, "Query %v fetched too many chunks (%d > %d)", matchers, len(filtered), maxChunksPerQuery)
		level.Error(log).Log("err", err)
		return nil, err
	}

	// Now fetch the actual chunk data from BOS
	keys := keysFromChunks(filtered)
	allChunks, err := ls.FetchChunks(ctx, filtered, keys)
	if err != nil {
		return nil, promql.ErrStorage{Err: err}
	}
	return allChunks, nil
}

// 返回未加载的chunk，只请求BTS
func (ls lokiStore) lookupChunkIDsByMatchersAndTags(ctx context.Context, userID string, from, through model.Time, matchers []*labels.Matcher, tags []*TagMatcher) ([]Chunk, error) {
	log, ctx := spanlogger.New(ctx, "LokiChunkStore.lookupChunksByMatchersAndTags")
	defer log.Finish()
	level.Debug(log).Log("from", from, "through", through, "matchers", len(matchers), "tags", len(tags))

	nq := len(matchers) + len(tags)
	if nq == 0 {
		return nil, fmt.Errorf("expected at least a matcher or a tag")
	}

	seen := map[string]struct{}{}
	queries := getIndexQueries(nq)
	defer putIndexQueries(queries)

	for _, m := range matchers {
		qs, _ := ls.schema.GetReadQueriesForMetricLabelValue(from, through, userID, "log", m.Name, m.Value)
		for _, q := range qs {
			key := q.TableName + ":" + q.HashValue
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				queries = append(queries, q)
			}
		}
	}

	for _, t := range tags {
		qs, _ := ls.schema.GetReadQueriesForTagHash(from, through, userID, t.Name, t.Value)
		for _, q := range qs {
			key := q.TableName + ":" + q.HashValue
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				queries = append(queries, q)
			}
		}
	}

	entries, err := ls.lookupEntriesByQueries(ctx, queries)
	if err != nil {
		return nil, err
	}

	chunkIDs := ls.parseLokiIndexEntries(ctx, entries, len(matchers), len(tags))
	return ls.convertChunkIDsToChunks(ctx, userID, chunkIDs)
}

// 只有满足所有的matchers和tags的chunkID才会留下
func (ls lokiStore) parseLokiIndexEntries(ctx context.Context, entries []IndexEntry, nMatchers, nTags int) []string {
	labelChunkIDCnts := make(map[string]int, nMatchers)
	tagChunkIDCnts := make(map[string]int, nTags)

	for _, e := range entries {
		info := ParseLokiIndexEntry(&e)
		if info == nil || len(info.ChunkID) == 0 {
			continue
		}

		switch info.IndexType {
		case LokiEntryKindLabelHash2StreamChunkIDs:
			labelChunkIDCnts[string(info.ChunkID)]++
		case LokiEntryKindTagHash2ChunkIDs:
			tagChunkIDCnts[string(info.ChunkID)]++
		}
	}

	if nTags == 0 {
		chunkIDs := make([]string, 0, len(entries))
		for chunkID, cnt := range labelChunkIDCnts {
			if cnt >= nMatchers {
				chunkIDs = append(chunkIDs, chunkID)
			}
		}
		sort.Strings(chunkIDs)
		return uniqueStrings(chunkIDs)
	}

	labelChunkIDs := make(map[string]bool, len(labelChunkIDCnts))
	for chunkID, cnt := range labelChunkIDCnts {
		if cnt >= nMatchers {
			labelChunkIDs[chunkID] = true
		}
	}

	chunkIDs := make([]string, 0, len(entries))
	for chunkID, cnt := range tagChunkIDCnts {
		if cnt >= nTags && labelChunkIDs[chunkID] {
			chunkIDs = append(chunkIDs, chunkID)
		}
	}
	sort.Strings(chunkIDs)
	return uniqueStrings(chunkIDs)
}

func (ls lokiStore) GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers []*labels.Matcher, tags []*TagMatcher) ([][]Chunk, []*Fetcher, error) {
	log, ctx := spanlogger.New(ctx, "LokiChunkStore.GetChunkRefs")
	defer log.Span.Finish()

	// Validate the query is within reasonable bounds.
	metricName, matchers, shortcut, err := ls.validateQuery(ctx, userID, &from, &through, matchers)
	if err != nil {
		return nil, nil, err
	} else if shortcut {
		return nil, nil, nil
	}

	level.Debug(log).Log("metric", metricName)

	chunks, err := ls.lookupChunkIDsByMatchersAndTags(ctx, userID, from, through, matchers, tags)
	if err != nil {
		return nil, nil, err
	}

	// Filter out chunks that are not in the selected time range.
	chunks = filterChunksByTime(from, through, chunks)

	return [][]Chunk{chunks}, []*Fetcher{ls.store.Fetcher}, nil
}

func (ls lokiStore) LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error) {
	shortcut, err := ls.validateQueryTimeRange(ctx, userID, &from, &through)
	if err != nil {
		return nil, err
	} else if shortcut {
		return nil, nil
	}

	queries, _ := ls.schema.GetReadQueriesForMetric(from, through, userID, "log")
	entries, err := ls.lookupEntriesByQueries(ctx, queries)
	if err != nil {
		return nil, err
	}

	labelNames := make([]string, 0, len(entries))
	for _, e := range entries {
		info := ParseLokiIndexEntry(&e)
		if info == nil || len(info.LabelName) == 0 {
			continue
		}
		labelNames = append(labelNames, info.LabelName)
	}
	sort.Strings(labelNames)
	return uniqueStrings(labelNames), nil

}

func (ls lokiStore) LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName, labelName string) ([]string, error) {
	shortcut, err := ls.validateQueryTimeRange(ctx, userID, &from, &through)
	if err != nil {
		return nil, err
	} else if shortcut {
		return nil, nil
	}

	queries, _ := ls.schema.GetReadQueriesForMetricLabel(from, through, userID, "log", labelName)
	entries, err := ls.lookupEntriesByQueries(ctx, queries)
	if err != nil {
		return nil, err
	}

	labelValues := make([]string, 0, len(entries))
	for _, e := range entries {
		info := ParseLokiIndexEntry(&e)
		if info == nil || len(info.LabelValue) == 0 {
			continue
		}
		labelValues = append(labelValues, info.LabelValue)
	}
	sort.Strings(labelValues)
	return uniqueStrings(labelValues), nil
}

func (ls lokiStore) TagNames(ctx context.Context, userID string, from, through model.Time) ([]string, error) {
	shortcut, err := ls.validateQueryTimeRange(ctx, userID, &from, &through)
	if err != nil {
		return nil, err
	} else if shortcut {
		return nil, nil
	}

	queries, _ := ls.schema.GetReadQueriesForTagName(from, through, userID)
	entries, err := ls.lookupEntriesByQueries(ctx, queries)
	if err != nil {
		return nil, err
	}

	tagNames := make([]string, 0, len(entries))
	for _, e := range entries {
		info := ParseLokiIndexEntry(&e)
		if info == nil || len(info.TagName) == 0 {
			continue
		}
		tagNames = append(tagNames, info.TagName)
	}
	sort.Strings(tagNames)
	return uniqueStrings(tagNames), nil
}

func (ls lokiStore) TagValues(ctx context.Context, userID string, from, through model.Time, tagName string) ([]string, error) {
	shortcut, err := ls.validateQueryTimeRange(ctx, userID, &from, &through)
	if err != nil {
		return nil, err
	} else if shortcut {
		return nil, nil
	}

	queries, _ := ls.schema.GetReadQueriesForTagValue(from, through, userID, tagName)
	entries, err := ls.lookupEntriesByQueries(ctx, queries)
	if err != nil {
		return nil, err
	}

	tagValues := make([]string, 0, len(entries))
	for _, e := range entries {
		info := ParseLokiIndexEntry(&e)
		if info == nil || len(info.TagValue) == 0 {
			continue
		}
		tagValues = append(tagValues, info.TagValue)
	}
	sort.Strings(tagValues)
	return uniqueStrings(tagValues), nil
}

var indexQueriesPool sync.Pool

func getIndexQueries(n int) []IndexQuery {
	v := indexQueriesPool.Get()
	if v == nil {
		return make([]IndexQuery, 0, n)
	}
	qs := v.([]IndexQuery)
	if nn := n - cap(qs); nn > 0 {
		qs = append(qs, make([]IndexQuery, 0, nn)...)
	}
	qs = qs[:0]
	return qs
}

func putIndexQueries(qs []IndexQuery) {
	indexQueriesPool.Put(qs)
}
