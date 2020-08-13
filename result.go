package transactions

import (
	gocb "github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type AttemptState int

const (
	AttemptStateNothingWritten AttemptState = AttemptState(coretxns.AttemptStateNothingWritten)
	AttemptStatePending        AttemptState = AttemptState(coretxns.AttemptStatePending)
	AttemptStateCommitted      AttemptState = AttemptState(coretxns.AttemptStateCommitted)
	AttemptStateCompleted      AttemptState = AttemptState(coretxns.AttemptStateCompleted)
	AttemptStateAborted        AttemptState = AttemptState(coretxns.AttemptStateAborted)
	AttemptStateRolledBack     AttemptState = AttemptState(coretxns.AttemptStateRolledBack)
)

// Attempt represents a singular attempt at executing a transaction.  A
// transaction may require multiple attempts before being successful.
type Attempt struct {
	State AttemptState
	ID    string
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

	// Internal: This should never be used and is not supported.
	Internal struct {
		MutationTokens []gocb.MutationToken
	}
}

func createResult(attempts []Attempt, attempt coretxns.Attempt, txnID string) *Result {
	state := &gocb.MutationState{}
	for _, tok := range attempt.MutationState {
		state.Internal().Add(tok.BucketName, tok.MutationToken)
	}

	return &Result{
		Attempts:          attempts,
		TransactionID:     txnID,
		UnstagingComplete: attempt.State == coretxns.AttemptStateCompleted,
		MutationState:     *state,
		Internal:          struct{ MutationTokens []gocb.MutationToken }{MutationTokens: state.Internal().Tokens()},
	}
}
