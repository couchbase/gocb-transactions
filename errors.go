// Copyright 2021 Couchbase
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transactions

import (
	"errors"
	coretxns "github.com/couchbase/gocbcore-transactions"
)

var (
	// ErrOther indicates an non-specific error has occured.
	ErrOther = coretxns.ErrOther

	// ErrTransient indicates a transient error occured which may succeed at a later point in time.
	ErrTransient = coretxns.ErrTransient

	// ErrWriteWriteConflict indicates that another transaction conflicted with this one.
	ErrWriteWriteConflict = coretxns.ErrWriteWriteConflict

	// ErrHard indicates that an unrecoverable error occured.
	ErrHard = coretxns.ErrHard

	// ErrAmbiguous indicates that a failure occured but the outcome was not known.
	ErrAmbiguous = coretxns.ErrAmbiguous

	// ErrAtrFull indicates that the ATR record was too full to accept a new mutation.
	ErrAtrFull = coretxns.ErrAtrFull

	// ErrAttemptExpired indicates an attempt expired
	ErrAttemptExpired = coretxns.ErrAttemptExpired

	// ErrAtrNotFound indicates that an expected ATR document was missing
	ErrAtrNotFound = coretxns.ErrAtrNotFound

	// ErrAtrEntryNotFound indicates that an expected ATR entry was missing
	ErrAtrEntryNotFound = coretxns.ErrAtrEntryNotFound

	// ErrDocAlreadyInTransaction indicates that a document is already in a transaction.
	ErrDocAlreadyInTransaction = coretxns.ErrDocAlreadyInTransaction

	// ErrTransactionAbortedExternally indicates the transaction was aborted externally.
	ErrTransactionAbortedExternally = coretxns.ErrTransactionAbortedExternally

	// ErrTransactionAbortedExternally indicates the transaction was aborted externally.
	ErrPreviousOperationFailed = coretxns.ErrPreviousOperationFailed

	// ErrForwardCompatibilityFailure indicates an operation failed due to involving a document in another transaction
	// which contains features this transaction does not support.
	ErrForwardCompatibilityFailure = coretxns.ErrForwardCompatibilityFailure

	// ErrIllegalState is used for when a transaction enters an illegal State.
	ErrIllegalState = coretxns.ErrIllegalState

	ErrDocumentNotFound = coretxns.ErrDocumentNotFound

	ErrDocumentAlreadyExists = coretxns.ErrDocumentAlreadyExists

	ErrAttemptNotFoundOnQuery = errors.New("attempt not found on query")

	ErrCasMismatch = coretxns.ErrCasMismatch
)

type TransactionFailedError struct {
	cause  error
	result *Result
}

func (tfe TransactionFailedError) Error() string {
	if tfe.cause == nil {
		return "transaction failed"
	}
	return "transaction failed | " + tfe.cause.Error()
}

func (tfe TransactionFailedError) Unwrap() error {
	return tfe.cause
}

// Internal: This should never be used and is not supported.
func (tfe TransactionFailedError) Result() *Result {
	return tfe.result
}

type TransactionExpiredError struct {
	result *Result
}

func (tfe TransactionExpiredError) Error() string {
	return ErrAttemptExpired.Error()
}

func (tfe TransactionExpiredError) Unwrap() error {
	return ErrAttemptExpired
}

// Internal: This should never be used and is not supported.
func (tfe TransactionExpiredError) Result() *Result {
	return tfe.result
}

type TransactionCommitAmbiguousError struct {
	cause  error
	result *Result
}

func (tfe TransactionCommitAmbiguousError) Error() string {
	if tfe.cause == nil {
		return "transaction commit ambiguous"
	}
	return "transaction failed | " + tfe.cause.Error()
}

func (tfe TransactionCommitAmbiguousError) Unwrap() error {
	return tfe.cause
}

// Internal: This should never be used and is not supported.
func (tfe TransactionCommitAmbiguousError) Result() *Result {
	return tfe.result
}

type TransactionFailedPostCommit struct {
	cause  error
	result *Result
}

func (tfe TransactionFailedPostCommit) Error() string {
	if tfe.cause == nil {
		return "transaction failed post commit"
	}
	return "transaction failed | " + tfe.cause.Error()
}

func (tfe TransactionFailedPostCommit) Unwrap() error {
	return tfe.cause
}

// Internal: This should never be used and is not supported.
func (tfe TransactionFailedPostCommit) Result() *Result {
	return tfe.result
}

// TransactionOperationFailedError is used when a transaction operation fails.
// Internal: This should never be used and is not supported.
type TransactionOperationFailedError struct {
	shouldRetry       bool
	shouldNotRollback bool
	errorCause        error
	shouldRaise       coretxns.ErrorReason
	errorClass        coretxns.ErrorClass
}

func (tfe TransactionOperationFailedError) Error() string {
	if tfe.errorCause == nil {
		return "transaction operation failed"
	}
	return "transaction operation failed | " + tfe.errorCause.Error()
}

func (tfe TransactionOperationFailedError) Unwrap() error {
	return tfe.errorCause
}

// Retry signals whether a new attempt should be made at rollback.
func (tfe TransactionOperationFailedError) Retry() bool {
	return tfe.shouldRetry
}

// Rollback signals whether the attempt should be auto-rolled back.
func (tfe TransactionOperationFailedError) Rollback() bool {
	return !tfe.shouldNotRollback
}

// ToRaise signals which error type should be raised to the application.
func (tfe TransactionOperationFailedError) ToRaise() coretxns.ErrorReason {
	return tfe.shouldRaise
}

// ErrorClass is the class of error which caused this error.
func (tfe *TransactionOperationFailedError) ErrorClass() coretxns.ErrorClass {
	return tfe.errorClass
}

func createTransactionOperationFailedError(err error) error {
	if err == nil {
		return nil
	}

	var txnErr *coretxns.TransactionOperationFailedError
	if errors.As(err, &txnErr) {
		return &TransactionOperationFailedError{
			shouldRetry:       txnErr.Retry(),
			shouldNotRollback: !txnErr.Rollback(),
			errorCause:        txnErr.Unwrap(),
			shouldRaise:       txnErr.ToRaise(),
			errorClass:        txnErr.ErrorClass(),
		}
	} else {
		return &TransactionOperationFailedError{
			errorCause: err,
			errorClass: coretxns.ErrorClassFailOther,
		}
	}
}

func errorReasonFromString(reason string) coretxns.ErrorReason {
	switch reason {
	case "failed":
		return coretxns.ErrorReasonTransactionFailed
	case "expired":
		return coretxns.ErrorReasonTransactionExpired
	case "commit_ambiguous":
		return coretxns.ErrorReasonTransactionCommitAmbiguous
	case "failed_post_commit":
		return coretxns.ErrorReasonTransactionFailedPostCommit
	default:
		return coretxns.ErrorReasonTransactionFailed
	}
}
