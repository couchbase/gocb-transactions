package transactions

import (
	"github.com/couchbase/gocb"
)

// GetResult represents the result of a Get operation which was performed.
type GetResult struct {
	cas        gocb.Cas
	transcoder gocb.Transcoder
	flags      uint32
	contents   []byte
}

// Content provides access to the documents contents.
func (d *GetResult) Content(valuePtr interface{}) error {
	return d.transcoder.Decode(d.contents, d.flags, valuePtr)
}
