package transactions

import (
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

// GetResult represents the result of a Get operation which was performed.
type GetResult struct {
	collection *gocb.Collection
	docID      string

	transcoder gocb.Transcoder
	flags      uint32

	coreRes *coretxns.GetResult
}

// Content provides access to the documents contents.
func (d *GetResult) Content(valuePtr interface{}) error {
	return d.transcoder.Decode(d.coreRes.Value, d.flags, valuePtr)
}
