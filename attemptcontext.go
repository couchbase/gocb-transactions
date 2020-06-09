package transactions

import (
	"errors"

	gocb "github.com/couchbase/gocb/v2"
)

// AttemptContext represents a single attempt to execute a transaction.
type AttemptContext struct {
}

// GetOptional will attempt to fetch a document, and return nil if it does not exist.
func (c *AttemptContext) GetOptional(collection *gocb.Collection, id string) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *AttemptContext) Get(collection *gocb.Collection, id string) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

// Replace will replace the contents of a document, failing if the document does not already exist.
func (c *AttemptContext) Replace(doc *GetResult, value interface{}) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

// Insert will insert a new document, failing if the document already exists.
func (c *AttemptContext) Insert(collection *gocb.Collection, id string, value interface{}) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

// Remove will delete a document.
func (c *AttemptContext) Remove(doc *GetResult) error {
	return errors.New("not implemented")
}

// Commit will attempt to commit the transaction in its entirety.
func (c *AttemptContext) Commit() error {
	return errors.New("not implemented")
}

// Rollback will undo all changes related to a transaction.
func (c *AttemptContext) Rollback() error {
	return errors.New("not implemented")
}

// Defer serializes the transaction to enable it to be completed at a later point in time.
// VOLATILE: This API is subject to change at any time.
func (c *AttemptContext) Defer() error {
	return errors.New("not implemented")
}
