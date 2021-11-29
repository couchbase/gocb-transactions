package transactions

// SingleQueryTransactionResult allows access to the results of a single query transaction.
type SingleQueryTransactionResult struct {
	wrapped           *QueryResult
	unstagingComplete bool
}

// QueryResult returns the result of the query.
func (qr *SingleQueryTransactionResult) QueryResult() *QueryResult {
	return qr.wrapped
}

// UnstagingComplete returns whether all documents were successfully unstaged (committed).
//
// This will only return true if the transaction reached the COMMIT point and then went on to reach
// the COMPLETE point.
//
// It will be false for transactions that:
// - Rolled back
// - Were read-only
func (qr *SingleQueryTransactionResult) UnstagingComplete() bool {
	return qr.unstagingComplete
}
