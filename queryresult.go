package transactions

import (
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbase/gocbcore-transactions"
	"time"
)

// QueryResult allows access to the results of a query.
type QueryResult struct {
	wrapped *gocb.QueryResult
	context *AttemptContext

	err error
}

func newQueryResult(wrapped *gocb.QueryResult, context *AttemptContext) *QueryResult {
	return &QueryResult{
		wrapped: wrapped,
		context: context,
	}
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *QueryResult) Next() bool {
	if r.wrapped == nil {
		return false
	}

	hasNext := r.wrapped.Next()
	if hasNext {
		return true
	}

	meta, err := r.wrapped.MetaData()
	if err != nil {
		r.err = err
	}

	if err := r.wrapped.Err(); err != nil {
		r.err = r.context.queryMaybeTranslateToTransactionsError(err)
		return false
	}

	if meta.Status == gocb.QueryStatusFatal {
		r.err = r.context.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:  true,
			Reason:          coretxns.ErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
		return false
	}

	return false
}

// Row returns the contents of the current row
func (r *QueryResult) Row(valuePtr interface{}) error {
	return r.wrapped.Row(valuePtr)
}

// Err returns any errors that have occurred on the stream
func (r *QueryResult) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.wrapped.Err()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *QueryResult) Close() error {
	err := r.wrapped.Close()
	if r.err != nil {
		return r.err
	}

	return err
}

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *QueryResult) One(valuePtr interface{}) error {
	// Prime the row
	if !r.Next() {
		if r.err != nil {
			return r.err
		}

		return gocb.ErrNoResult
	}

	err := r.Row(valuePtr)
	if err != nil {
		return err
	}

	for r.Next() {
	}
	if r.err != nil {
		return r.err
	}

	return nil
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *QueryResult) MetaData() (*QueryMetaData, error) {
	meta, err := r.wrapped.MetaData()
	if err != nil {
		return nil, err
	}

	warnings := make([]QueryWarning, len(meta.Warnings))
	for i, w := range meta.Warnings {
		warnings[i] = QueryWarning{
			Code:    w.Code,
			Message: w.Message,
		}
	}

	return &QueryMetaData{
		RequestID:       meta.RequestID,
		ClientContextID: meta.ClientContextID,
		Status:          QueryStatus(meta.Status),
		Metrics: QueryMetrics{
			ElapsedTime:   meta.Metrics.ElapsedTime,
			ExecutionTime: meta.Metrics.ExecutionTime,
			ResultCount:   meta.Metrics.ResultCount,
			ResultSize:    meta.Metrics.ResultSize,
			MutationCount: meta.Metrics.MutationCount,
			SortCount:     meta.Metrics.SortCount,
			ErrorCount:    meta.Metrics.ErrorCount,
			WarningCount:  meta.Metrics.WarningCount,
		},
		Signature: meta.Signature,
		Warnings:  warnings,
		Profile:   meta.Profile,
	}, nil
}

// QueryMetaData provides access to the meta-data properties of a query result.
type QueryMetaData struct {
	RequestID       string
	ClientContextID string
	Status          QueryStatus
	Metrics         QueryMetrics
	Signature       interface{}
	Warnings        []QueryWarning
	Profile         interface{}
}

// QueryWarning encapsulates any warnings returned by a query.
type QueryWarning struct {
	Code    uint32
	Message string
}

// QueryMetrics encapsulates various metrics gathered during a queries execution.
type QueryMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint64
	ResultSize    uint64
	MutationCount uint64
	SortCount     uint64
	ErrorCount    uint64
	WarningCount  uint64
}

// QueryStatus provides information about the current status of a query.
type QueryStatus string

const (
	// QueryStatusRunning indicates the query is still running
	QueryStatusRunning QueryStatus = QueryStatus(gocb.QueryStatusRunning)

	// QueryStatusSuccess indicates the query was successful.
	QueryStatusSuccess QueryStatus = QueryStatus(gocb.QueryStatusSuccess)

	// QueryStatusErrors indicates a query completed with errors.
	QueryStatusErrors QueryStatus = QueryStatus(gocb.QueryStatusErrors)

	// QueryStatusCompleted indicates a query has completed.
	QueryStatusCompleted QueryStatus = QueryStatus(gocb.QueryStatusCompleted)

	// QueryStatusStopped indicates a query has been stopped.
	QueryStatusStopped QueryStatus = QueryStatus(gocb.QueryStatusStopped)

	// QueryStatusTimeout indicates a query timed out.
	QueryStatusTimeout QueryStatus = QueryStatus(gocb.QueryStatusTimeout)

	// QueryStatusClosed indicates that a query was closed.
	QueryStatusClosed QueryStatus = QueryStatus(gocb.QueryStatusClosed)

	// QueryStatusFatal indicates that a query ended with a fatal error.
	QueryStatusFatal QueryStatus = QueryStatus(gocb.QueryStatusFatal)

	// QueryStatusAborted indicates that a query was aborted.
	QueryStatusAborted QueryStatus = QueryStatus(gocb.QueryStatusAborted)

	// QueryStatusUnknown indicates that the query status is unknown.
	QueryStatusUnknown QueryStatus = QueryStatus(gocb.QueryStatusUnknown)
)
