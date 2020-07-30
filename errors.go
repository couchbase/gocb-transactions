package transactions

import (
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type FailureReason uint8

const (
	FailureReasonFailed FailureReason = iota
	FailureReasonExpired
)

var (
	// ErrOther indicates an non-specific error has occured.
	ErrOther = coretxns.ErrOther

	// ErrTransient indicates a transient error occured which may succeed at a later point in time.
	ErrTransient = coretxns.ErrTransient

	// ErrDocNotFound indicates that a needed document was not found.
	ErrDocNotFound = coretxns.ErrDocNotFound

	// ErrDocAlreadyExists indicates that a document already existed unexpectedly.
	ErrDocAlreadyExists = coretxns.ErrDocAlreadyExists

	// ErrPathNotFound indicates that a needed path was not found.
	ErrPathNotFound = coretxns.ErrPathNotFound

	// ErrPathAlreadyExists indicates that a path already existed unexpectedly.
	ErrPathAlreadyExists = coretxns.ErrPathAlreadyExists

	// ErrWriteWriteConflict indicates that another transaction conflicted with this one.
	ErrWriteWriteConflict = coretxns.ErrWriteWriteConflict

	// ErrCasMismatch indicates that a cas mismatch occured during a store operation.
	ErrCasMismatch = coretxns.ErrCasMismatch

	// ErrHard indicates that an unrecoverable error occured.
	ErrHard = coretxns.ErrHard

	// ErrAmbiguous indicates that a failure occured but the outcome was not known.
	ErrAmbiguous = coretxns.ErrAmbiguous

	// ErrExpiry indicates that an operation expired before completion.
	ErrExpiry = coretxns.ErrExpiry

	// ErrAtrFull indicates that the ATR record was too full to accept a new mutation.
	ErrAtrFull = coretxns.ErrAtrFull

	// ErrAttemptExpired indicates an attempt expired
	ErrAttemptExpired = coretxns.ErrAttemptExpired

	// ErrAtrNotFound indicates that an expected ATR document was missing
	ErrAtrNotFound = coretxns.ErrAtrNotFound

	// ErrAtrEntryNotFound indicates that an expected ATR entry was missing
	ErrAtrEntryNotFound = coretxns.ErrAtrEntryNotFound

	// ErrUhOh is used for now to describe errors I yet know how to categorize
	ErrUhOh = coretxns.ErrUhOh
)
