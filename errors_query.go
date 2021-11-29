package transactions

import (
	"encoding/json"
	"errors"
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbase/gocbcore-transactions"
)

func (c *AttemptContext) queryErrorCodeToError(code uint32) error {
	switch code {
	case 1065:
		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         coretxns.ErrorReasonTransactionFailed,
			ErrorCause:     gocb.ErrFeatureNotAvailable,
		})
	case 1080:
		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:    true,
			Reason:            coretxns.ErrorReasonTransactionExpired,
			ErrorCause:        coretxns.ErrAttemptExpired,
			ErrorClass:        coretxns.ErrorClassFailExpiry,
			ShouldNotRollback: true,
		})
	case 17004:
		return ErrAttemptNotFoundOnQuery
	case 17010:
		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:    true,
			Reason:            coretxns.ErrorReasonTransactionExpired,
			ErrorCause:        coretxns.ErrAttemptExpired,
			ErrorClass:        coretxns.ErrorClassFailExpiry,
			ShouldNotRollback: true,
		})
	case 17012:
		return ErrDocumentAlreadyExists
	case 17014:
		return ErrDocumentNotFound
	case 17015:
		return ErrCasMismatch
	default:
		return nil
	}
}

func (c *AttemptContext) queryCauseToOperationFailedError(queryErr *gocb.QueryError) error {

	var operationFailedErrs []jsonQueryTransactionOperationFailedCause
	if err := json.Unmarshal([]byte(queryErr.ErrorText), &operationFailedErrs); err == nil {
		for _, operationFailedErr := range operationFailedErrs {
			if operationFailedErr.Cause != nil {
				if operationFailedErr.Code >= 17000 && operationFailedErr.Code <= 18000 {
					if err := c.queryErrorCodeToError(operationFailedErr.Code); err != nil {
						return err
					}
				}

				return c.operationFailed(queryOperationFailedDef{
					ShouldNotRetry:    !operationFailedErr.Cause.Retry,
					ShouldNotRollback: !operationFailedErr.Cause.Rollback,
					Reason:            errorReasonFromString(operationFailedErr.Cause.Raise),
					ErrorCause:        queryErr,
					ShouldNotCommit:   true,
				})
			}
		}
	}
	return nil
}

func (c *AttemptContext) queryMaybeTranslateToTransactionsError(err error) error {
	if errors.Is(err, gocb.ErrTimeout) {
		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         coretxns.ErrorReasonTransactionExpired,
			ErrorCause:     coretxns.ErrAttemptExpired,
		})
	}

	var queryErr *gocb.QueryError
	if !errors.As(err, &queryErr) {
		return err
	}

	if len(queryErr.Errors) == 0 {
		return queryErr
	}

	// If an error contains a cause field, use that error.
	// Otherwise, if an error has code between 17000 and 18000 inclusive, it is a transactions-related error. Use that.
	// Otherwise, fallback to using the first error.
	if err := c.queryCauseToOperationFailedError(queryErr); err != nil {
		return err
	}

	for _, e := range queryErr.Errors {
		if e.Code >= 17000 && e.Code <= 18000 {
			if err := c.queryErrorCodeToError(e.Code); err != nil {
				return err
			}
		}
	}

	if err := c.queryErrorCodeToError(queryErr.Errors[0].Code); err != nil {
		return err
	}

	return queryErr
}

type queryOperationFailedDef struct {
	ShouldNotRetry    bool
	ShouldNotRollback bool
	Reason            coretxns.ErrorReason
	ErrorCause        error
	ErrorClass        coretxns.ErrorClass
	ShouldNotCommit   bool
}

func (c *AttemptContext) operationFailed(def queryOperationFailedDef) *TransactionOperationFailedError {
	err := &TransactionOperationFailedError{
		shouldRetry:       !def.ShouldNotRetry,
		shouldNotRollback: def.ShouldNotRollback,
		errorCause:        def.ErrorCause,
		shouldRaise:       def.Reason,
		errorClass:        def.ErrorClass,
	}

	opts := coretxns.UpdateStateOptions{}
	if def.ShouldNotRollback {
		opts.ShouldNotRollback = true
	}
	if def.ShouldNotRetry {
		opts.ShouldNotRetry = true
	}
	if def.Reason == coretxns.ErrorReasonTransactionExpired {
		opts.HasExpired = true
	}
	if def.ShouldNotCommit {
		opts.ShouldNotCommit = true
	}
	c.txn.UpdateState(opts)

	return err
}
