package transactions

import (
	gocb "github.com/couchbase/gocb/v2"
)

// Attempt represents a singular attempt at executing a transaction.  A
// transaction may require multiple attempts before being successful.
type Attempt struct {
}

// Result represents the result of a transaction which was executed.
type Result struct {
	// TransactionID represents the UUID assigned to this transaction
	TransactionID string

	// Attempts records all attempts that were performed when executing
	// this transaction.
	Attempts []Attempt

	// MutationState represents the state associated with this transaction
	// and can be used to perform RYOW queries at a later point.
	MutationState gocb.MutationState

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool

	// Serialized represents the serialized data from this transaction if
	// the transaction was serialized as opposed to being executed.
	Serialized *SerializedContext
}
