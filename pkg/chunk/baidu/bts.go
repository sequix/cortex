package baidu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/baidubce/bce-sdk-go/auth"
	"github.com/baidubce/bce-sdk-go/http"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	chunkUtil "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	maxBatchPutRows    = 200
	maxBatchGetRows    = 100
	maxRequestBodySize = 10 * 1024 * 1024
	maxScanRows        = 10000
	maxScanRspSize     = 8 * 1024 * 1024

	columnName          = "labelValue"
	rowkeySeperator     = "\x00"
	rowkeySeperatorNext = "\x01"
)

var (
	signer = &auth.BceV1Signer{}
)

var (
	btsRequestDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "cortex",
		Name:       "baidu_bts_request_duration_seconds",
		Help:       "Time spent doing bos requests.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"operation", "status_code"})

	btsEntriesCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "baidu_bts_entries_total",
		Help:      "How many index entries sent to BTS",
	})
)

// Configuration for Baidu Table Storage (BTS).
type BTSConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"ak"`
	SecretKey string `yaml:"sk"`
	Instance  string `yaml:"instance"`
	Table     string `yaml:"table"`
}

type BTSClient struct {
	credential    *auth.BceCredentials
	endpoint      string
	uri           string
	headersToSign map[string]struct{}
}

func NewIndexClient(c BTSConfig) (chunk.IndexClient, error) {
	cred, err := auth.NewBceCredentials(c.AccessKey, c.SecretKey)
	if err != nil {
		return nil, err
	}

	// BTSClient的client的endpoint前不能加协议
	endpoint := c.Endpoint
	switch {
	case strings.HasPrefix(endpoint, "http://"):
		endpoint = endpoint[len("http://"):]
	case strings.HasPrefix(endpoint, "https://"):
		endpoint = endpoint[len("https://"):]
	}

	client := &BTSClient{
		credential: cred,
		endpoint:   c.Endpoint,
		uri:        "/v1/instance/" + c.Instance + "/table/" + c.Table + "/rows",
		headersToSign: map[string]struct{}{
			"host":       {},
			"x-bce-date": {},
		},
	}
	return client, nil
}

// Stop implements chunk.IndexClient interface.
func (c *BTSClient) Stop() {
}

func (c *BTSClient) NewWriteBatch() chunk.WriteBatch {
	return &btsBatchPutRowsReq{}
}

func (c *BTSClient) BatchWrite(ctx context.Context, wb chunk.WriteBatch) error {
	rows := wb.(*btsBatchPutRowsReq).Rows
	return parallelWrite(ctx, rows, maxBatchPutRows, c.write)
}

// TODO size limit
func parallelWrite(ctx context.Context, rows []btsRow, max int, f func(context.Context, []btsRow) error) error {
	errCh := make(chan error)
	defer close(errCh)

	n := len(rows)
	concurrency := 0

	for i := 0; i < n; i += max {
		j := i + max
		if j > n {
			j = n
		}

		go func(left, right int) {
			err := f(ctx, rows[left:right])
			if err == nil {
				btsEntriesCounter.Add(float64(right - left))
			}
			errCh <- err
		}(i, j)
		concurrency += 1
	}

	var lastErr error
	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (c *BTSClient) write(ctx context.Context, rows []btsRow) error {
	now := time.Now().UTC()

	reqBody, err := json.Marshal(&btsBatchPutRowsReq{Rows: rows})
	if err != nil {
		return fmt.Errorf("bts marshal put rows req failed: %s", err)
	}

	rsp, err := c.do(ctx, now, http.PUT, reqBody)
	if err != nil {
		return fmt.Errorf("bts put rows req failed: %s", err)
	}
	if err := rsp.Body().Close(); err != nil {
		return fmt.Errorf("bts put rows close rsp body failed: %s", err)
	}
	return nil
}

func (c *BTSClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return chunkUtil.DoParallelQueries(ctx, c.query, queries, callback)
}

func (c *BTSClient) query(ctx context.Context, query chunk.IndexQuery, callback func(result chunk.ReadBatch) (shouldContinue bool)) error {
	now := time.Now().UTC()

	var (
		valueEqual       = string(query.ValueEqual)
		hashSepLength    = len(query.HashValue) + 1
		start            = query.HashValue + rowkeySeperator
		rowkeyPrefix     = query.HashValue + rowkeySeperator
		rowkeyPrefixNext = query.HashValue + rowkeySeperatorNext
	)

	switch {
	case len(query.RangeValueStart) > 0:
		start = start + string(query.RangeValueStart)
	case len(query.RangeValuePrefix) > 0:
		start = start + string(query.RangeValuePrefix)
		rowkeyPrefix = start
	}

	req := &btsScanRowsReq{
		StartRowkey:  encoding.RawUrlEncode(start),
		IncludeStart: true,
		StopRowkey:   encoding.RawUrlEncode(rowkeyPrefixNext),
		IncludeStop:  false,
		Selector: []btsColumn{
			{Column: columnName},
		},
		Limit: maxScanRows,
	}

	for {
		reqBody, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf(" bts marshal scan req failed: %s", err)
		}

		base := time.Now()
		rsp, err := c.do(ctx, now, http.GET, reqBody)
		level.Debug(util.Logger).Log("module", "BTSClient", "msg", "Scan Rows", "size", len(reqBody), "duration", time.Since(base), "err", err)
		if err != nil {
			return fmt.Errorf("bts scan request failed: %s", err)
		}

		body, err := ioutil.ReadAll(rsp.Body())
		if err != nil {
			return fmt.Errorf("bts readall from rsp body failed: %s", err)
		}

		if err := rsp.Body().Close(); err != nil {
			level.Error(util.Logger).Log("msg", "bts scan close rsp body failed", err, err)
		}

		scanRsp := btsScanRowsRsp{}
		if err := json.Unmarshal(body, &scanRsp); err != nil {
			return fmt.Errorf("bts unmarshal scan rsp failed: %s", err)
		}

		rb := getBTSReadBatch()
		rows := scanRsp.Result
		for i := range rows {
			r := rows[i]
			rk := encoding.RawUrlDecode(r.Rowkey)

			if !strings.HasPrefix(rk, rowkeyPrefix) {
				scanRsp.NextStartKey = ""
				break
			}

			value := ""
			if len(r.Cells) > 0 {
				value = encoding.RawUrlDecode(r.Cells[0].Value)
			}
			if len(valueEqual) > 0 && valueEqual != value {
				continue
			}

			rangeValue := rk[hashSepLength:]
			rb.rangeValues = append(rb.rangeValues, []byte(rangeValue))
			rb.values = append(rb.values, []byte(value))
		}

		if len(rb.values) > 0 {
			if !callback(rb) {
				putBTSReadBatch(rb)
				break
			}
		}
		putBTSReadBatch(rb)

		if scanRsp.NextStartKey == "" {
			break
		}
	}
	return nil
}

func (c *BTSClient) do(ctx context.Context, now time.Time, method string, body []byte) (*http.Response, error) {
	req := &http.Request{}
	req.SetProtocol("https")
	req.SetEndpoint(c.endpoint)
	req.SetHeader("host", c.endpoint)
	req.SetHeader("x-bce-date", now.Format(time.RFC3339))
	req.SetHeader("Content-Type", "application/json")
	//req.SetHeader("Accept", "application/json")
	req.SetUri(c.uri)
	req.SetMethod(method)
	req.SetLength(int64(len(body)))
	req.SetBody(ioutil.NopCloser(bytes.NewReader(body)))

	signer.Sign(req, c.credential, &auth.SignOptions{
		HeadersToSign: c.headersToSign,
		Timestamp:     now.Unix(),
		ExpireSeconds: 180,
	})

	operation := "BTSBatchPutRows"
	if method == http.GET {
		operation = "BTSScanRows"
	}

	base := time.Now()
	rsp, err := http.Execute(req)

	statusCode := "-1"
	if rsp != nil {
		statusCode = strconv.FormatInt(int64(rsp.StatusCode()), 10)
	}
	btsRequestDuration.WithLabelValues(operation, statusCode).Observe(time.Since(base).Seconds())

	if err != nil {
		return rsp, err
	}

	if rsp.StatusCode() > 399 {
		content, err := ioutil.ReadAll(io.LimitReader(rsp.Body(), 256))
		if err != nil {
			return nil, fmt.Errorf("BTSClient reading response body failed")
		}
		return nil, fmt.Errorf("BTSClient %s rows: %s", req.Method(), content)
	}
	return rsp, nil
}
