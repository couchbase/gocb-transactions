package transactions

import (
	"time"

	"github.com/couchbase/gocb"
)

type GetResult struct {
	cas        gocb.Cas
	transcoder gocb.Transcoder
	flags      uint32
	contents   []byte
	expiry     *time.Duration
}
