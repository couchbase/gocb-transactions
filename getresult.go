package transactions

import (
	"github.com/couchbase/gocb"
)

type GetResult struct {
	cas        gocb.Cas
	transcoder gocb.Transcoder
	flags      uint32
	contents   []byte
}

func (d *GetResult) Content(valuePtr interface{}) error {
	return d.transcoder.Decode(d.contents, d.flags, valuePtr)
}
