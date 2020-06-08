package transactions

import "github.com/couchbase/gocb"

type Attempt struct {
}

type Result struct {
	TransactionId     string
	Attempts          []Attempt
	MutationState     gocb.MutationState
	UnstagingComplete bool
	Serialized        *SerializedContext
}
