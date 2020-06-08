package transactions

import (
	"errors"

	gocb "github.com/couchbase/gocb/v2"
)

type AttemptContext struct {
}

func (c *AttemptContext) GetOptional(collection *gocb.Collection, id string) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

func (c *AttemptContext) Get(collection *gocb.Collection, id string) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

func (c *AttemptContext) Replace(doc *GetResult, value interface{}) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

func (c *AttemptContext) Insert(collection *gocb.Collection, id string, value interface{}) (*GetResult, error) {
	return nil, errors.New("not implemented")
}

func (c *AttemptContext) Remove(doc *GetResult) error {
	return errors.New("not implemented")
}

func (c *AttemptContext) Commit() error {
	return errors.New("not implemented")
}

func (c *AttemptContext) Rollback() error {
	return errors.New("not implemented")
}

// @Volatile
func (c *AttemptContext) Defer() error {
	return errors.New("not implemented")
}
