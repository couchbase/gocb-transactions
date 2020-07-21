package transactions

import (
	"errors"
	"log"

	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

// AttemptContext represents a single attempt to execute a transaction.
type AttemptContext struct {
	txn        *coretxns.Transaction
	transcoder gocb.Transcoder

	committed  bool
	rolledBack bool
}

// GetOptional will attempt to fetch a document, and return nil if it does not exist.
func (c *AttemptContext) GetOptional(collection *gocb.Collection, id string) (*GetResult, error) {
	res, err := c.Get(collection, id)
	if err == ErrDocNotFound {
		return nil, nil
	}
	return res, err
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *AttemptContext) Get(collection *gocb.Collection, id string) (resOut *GetResult, errOut error) {
	a, err := collection.Bucket().Internal().IORouter()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{}, 1)
	err = c.txn.Get(coretxns.GetOptions{
		Agent:          a,
		ScopeName:      collection.ScopeName(),
		CollectionName: collection.Name(),
		Key:            []byte(id),
	}, func(res *coretxns.GetResult, err error) {
		if err == nil {
			resOut = &GetResult{
				collection: collection,
				docID:      id,

				transcoder: gocb.NewJSONTranscoder(),
				flags:      2 << 24,

				coreRes: res,
			}

			log.Printf("DOC:%v", resOut)
		}
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh

	return
}

// Replace will replace the contents of a document, failing if the document does not already exist.
func (c *AttemptContext) Replace(doc *GetResult, value interface{}) (resOut *GetResult, errOut error) {
	collection := doc.collection
	id := doc.docID

	// TODO: Use Transcoder here
	valueBytes, _, err := c.transcoder.Encode(value)
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{}, 1)
	err = c.txn.Replace(coretxns.ReplaceOptions{
		Document: doc.coreRes,
		Value:    valueBytes,
	}, func(res *coretxns.GetResult, err error) {
		if err == nil {
			resOut = &GetResult{
				collection: collection,
				docID:      id,

				transcoder: gocb.NewJSONTranscoder(),
				flags:      2 << 24,

				coreRes: res,
			}

			log.Printf("DOC:%v", resOut)
		}
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh

	return
}

// Insert will insert a new document, failing if the document already exists.
func (c *AttemptContext) Insert(collection *gocb.Collection, id string, value interface{}) (resOut *GetResult, errOut error) {
	a, err := collection.Bucket().Internal().IORouter()
	if err != nil {
		return nil, err
	}

	// TODO: Use Transcoder here
	valueBytes, _, err := c.transcoder.Encode(value)
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{}, 1)
	err = c.txn.Insert(coretxns.InsertOptions{
		Agent:          a,
		ScopeName:      collection.ScopeName(),
		CollectionName: collection.Name(),
		Key:            []byte(id),
		Value:          valueBytes,
	}, func(res *coretxns.GetResult, err error) {
		if err == nil {
			resOut = &GetResult{
				collection: collection,
				docID:      id,

				transcoder: gocb.NewJSONTranscoder(),
				flags:      2 << 24,

				coreRes: res,
			}

			log.Printf("DOC:%v", resOut)
		}
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh

	return
}

// Remove will delete a document.
func (c *AttemptContext) Remove(doc *GetResult) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.Remove(coretxns.RemoveOptions{
		Document: doc.coreRes,
	}, func(res *coretxns.GetResult, err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh

	return
}

// Commit will attempt to commit the transaction in its entirety.
func (c *AttemptContext) Commit() (errOut error) {
	c.committed = true
	waitCh := make(chan struct{}, 1)
	err := c.txn.Commit(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

// Rollback will undo all changes related to a transaction.
func (c *AttemptContext) Rollback() (errOut error) {
	c.rolledBack = true
	waitCh := make(chan struct{}, 1)
	err := c.txn.Rollback(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

// Defer serializes the transaction to enable it to be completed at a later point in time.
// VOLATILE: This API is subject to change at any time.
func (c *AttemptContext) Defer() error {
	return errors.New("not implemented")
}
