package baidu

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
)

type btsBatchPutRowsReq struct {
	Rows []btsRow `json:"rows"`
}

// Add implements chunk.WriteBatch.
func (r *btsBatchPutRowsReq) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	r.Rows = append(r.Rows, btsRow{
		Rowkey: encoding.RawUrlEncode(hashValue + rowkeySeperator + string(rangeValue)),
		Cells: []btsCell{
			{
				Column: columnName,
				Value:  encoding.RawUrlEncode(string(value)),
			},
		},
	})
}

type btsBatchGetRowsReq struct {
	Rows []btsRow `json:"rows"`
}

type btsBatchGetRowsRsp struct {
	Result []btsRow `json:"result"`
}

type btsScanRowsReq struct {
	StartRowkey  string      `json:"startRowkey,omitempty"`
	IncludeStart bool        `json:"includeStart,omitempty"`
	StopRowkey   string      `json:"stopRowkey,omitempty"`
	IncludeStop  bool        `json:"includeStop,omitempty"`
	Selector     []btsColumn `json:"selector,omitempty"`
	Limit        int         `json:"limit,omitempty"`
}

type btsScanRowsRsp struct {
	Result       []btsRow `json:"result"`
	NextStartKey string   `json:"nextStartKey,omitempty"`
}

type btsRow struct {
	Rowkey string    `json:"rowkey,omitempty"`
	Cells  []btsCell `json:"cells,omitempty"`
}

var btsRowsPool sync.Pool

func getBTSRows(n int) []btsRow {
	v := btsRowsPool.Get()
	if v == nil {
		return make([]btsRow, 0, n)
	}
	r := v.([]btsRow)
	if nn := n - cap(r); nn > 0 {
		r = append(r, make([]btsRow, nn)...)
	}
	return r[:0]
}

func putBTSRows(r []btsRow) {
	btsRowsPool.Put(r)
}

type btsCell struct {
	Column string `json:"column,omitempty"`
	Value  string `json:"value,omitempty"`
	//Timestamp time.Time `json:"timestamp,omitempty"`
}

type btsColumn struct {
	Column string `json:"column,omitempty"`
}

type btsReadBatch struct {
	rangeValues [][]byte
	values      [][]byte
}

func (r *btsReadBatch) Iterator() chunk.ReadBatchIterator {
	return &btsReadBatchIterator{
		btsReadBatch: r,
	}
}

func (r *btsReadBatch) reset() {
	r.rangeValues = r.rangeValues[:0]
	r.values = r.values[:0]
}

var btsReadBatchPool sync.Pool

func getBTSReadBatch() *btsReadBatch {
	v := btsReadBatchPool.Get()
	if v == nil {
		return &btsReadBatch{}
	}
	r := v.(*btsReadBatch)
	r.reset()
	return r
}

func putBTSReadBatch(r *btsReadBatch) {
	btsReadBatchPool.Put(r)
}

type btsReadBatchIterator struct {
	*btsReadBatch
}

func (ri *btsReadBatchIterator) Next() bool {
	return len(ri.rangeValues) > 0
}

func (ri *btsReadBatchIterator) RangeValue() []byte {
	rst := ri.rangeValues[0]
	ri.rangeValues = ri.rangeValues[1:]
	return rst
}

func (ri *btsReadBatchIterator) Value() []byte {
	rst := ri.values[0]
	ri.values = ri.values[1:]
	return rst
}
