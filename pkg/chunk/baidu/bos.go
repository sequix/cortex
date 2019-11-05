package baidu

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/baidubce/bce-sdk-go/services/bos"
	bosAPI "github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

var (
	bosRequestDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "cortex",
		Name:       "baidu_bos_request_duration_seconds",
		Help:       "Time spent doing bos requests.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"operation", "success"})
)

type BOSClient struct {
	bos    *bos.Client
	bucket string
	prefix string
}

// Configuration for Baidu Object Storage (BOS).
type BOSConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"ak"`
	SecretKey string `yaml:"sk"`
	Bucket    string `yaml:"bucket"`
	Prefix    string `yaml:"prefix"`
}

// NewObjectClient return bos-backed object btsClient.
func NewObjectClient(c BOSConfig) (chunk.ObjectClient, error) {
	bosClient, err := bos.NewClient(c.AccessKey, c.SecretKey, c.Endpoint)
	if err != nil {
		return nil, err
	}
	return &BOSClient{
		bos:    bosClient,
		bucket: c.Bucket,
		prefix: c.Prefix,
	}, nil
}

// Stop implements chunk.ObjectClient interface.
func (client *BOSClient) Stop() {
	// nothing to do
}

// PutChunks implements chunk.ObjectClient interface, put chunks parallelly.
func (client *BOSClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	incomingErrors := make(chan error)
	for i := range chunks {
		go func(i int) {
			buf, err := chunks[i].Encoded()
			if err != nil {
				incomingErrors <- err
				return
			}
			incomingErrors <- client.putChunk(ctx, client.prefix+chunks[i].ExternalKey(), buf)
		}(i)
	}

	var lastErr error
	for range chunks {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (client *BOSClient) putChunk(ctx context.Context, key string, buf []byte) error {
	base := time.Now()
	_, err := client.bos.PutObjectFromBytes(client.bucket, key, buf, nil)
	duration := time.Since(base).Seconds()
	if err == nil {
		bosRequestDuration.WithLabelValues("BOSPutObject", "1").Observe(duration)
	} else {
		bosRequestDuration.WithLabelValues("BOSGetObject", "0").Observe(duration)
	}
	return err
}

// GutChunks implements chunk.ObjectClient interface, get chunks parallelly.
func (client *BOSClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, chunks, client.getChunk)
}

func (client *BOSClient) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	var result *bosAPI.GetObjectResult
	key := client.prefix + c.ExternalKey()

	base := time.Now()
	result, err := client.bos.BasicGetObject(client.bucket, key)
	duration := time.Since(base).Seconds()
	if err != nil {
		bosRequestDuration.WithLabelValues("BOSGetObject", "0").Observe(duration)
		return chunk.Chunk{}, err
	}
	bosRequestDuration.WithLabelValues("BOSGetObject", "1").Observe(duration)
	defer result.Body.Close()

	buf, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return chunk.Chunk{}, err
	}
	if err := c.Decode(decodeContext, buf); err != nil {
		return chunk.Chunk{}, err
	}
	return c, nil
}
