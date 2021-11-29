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
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbase/gocbcore-transactions"
	"sync"
)

type attempt struct {
	State                 AttemptState
	PreExpiryAutoRollback bool
	Expired               bool
}

type queryState struct {
	queryTarget string
}

// AttemptContext represents a single attempt to execute a transaction.
type AttemptContext struct {
	txn        *coretxns.Transaction
	transcoder gocb.Transcoder
	cluster    *gocb.Cluster
	hooks      TransactionHooks

	userInvokedRollback bool

	// State applicable to when we move into query mode
	queryState *queryState
	// Pointer to satisfy go vet complaining about the hooks.
	queryStateLock *sync.Mutex
	queryConfig    PerTransactionQueryConfig
}

func (c *AttemptContext) canCommit() bool {
	return c.txn.CanCommit()
}

func (c *AttemptContext) shouldRollback() bool {
	return c.txn.ShouldRollback()
}

func (c *AttemptContext) shouldRetry() bool {
	return c.txn.ShouldRetry()
}

func (c *AttemptContext) attempt() attempt {
	a := c.txn.Attempt()
	return attempt{
		State:                 AttemptState(a.State),
		PreExpiryAutoRollback: a.PreExpiryAutoRollback,
		Expired:               c.txn.TimeRemaining() <= 0 || a.Expired,
	}
}

// Internal is used for internal dealings.
// Internal: This should never be used and is not supported.
func (c *AttemptContext) Internal() *InternalAttemptContext {
	return &InternalAttemptContext{
		ac: c,
	}
}

// InternalAttemptContext is used for internal dealings.
// Internal: This should never be used and is not supported.
type InternalAttemptContext struct {
	ac *AttemptContext
}

func (iac *InternalAttemptContext) IsExpired() bool {
	return iac.ac.txn.HasExpired()
}

// GetOptional will attempt to fetch a document, and return nil if it does not exist.
func (c *AttemptContext) GetOptional(collection *gocb.Collection, id string) (*GetResult, error) {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.getQueryMode(collection, id)
		if err != nil {
			if errors.Is(err, coretxns.ErrDocumentNotFound) {
				c.queryStateLock.Unlock()
				return nil, nil
			}
			c.txn.UpdateState(coretxns.UpdateStateOptions{
				ShouldNotCommit: true,
			})
			c.queryStateLock.Unlock()
			return nil, err
		}
		c.queryStateLock.Unlock()
		return res, nil
	}
	c.queryStateLock.Unlock()
	res, err := c.Get(collection, id)
	if errors.Is(err, coretxns.ErrDocumentNotFound) {
		return nil, nil
	}
	return res, err
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *AttemptContext) Get(collection *gocb.Collection, id string) (*GetResult, error) {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.getQueryMode(collection, id)
		if err != nil {
			c.txn.UpdateState(coretxns.UpdateStateOptions{
				ShouldNotCommit: true,
			})
			c.queryStateLock.Unlock()
			return nil, err
		}
		c.queryStateLock.Unlock()
		return res, nil
	}
	c.queryStateLock.Unlock()

	return c.get(collection, id)
}

func (c *AttemptContext) get(collection *gocb.Collection, id string) (resOut *GetResult, errOut error) {
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
		}
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Replace will replace the contents of a document, failing if the document does not already exist.
func (c *AttemptContext) Replace(doc *GetResult, value interface{}) (*GetResult, error) {
	// TODO: Use Transcoder here
	valueBytes, _, err := c.transcoder.Encode(value)
	if err != nil {
		return nil, err
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.replaceQueryMode(doc, valueBytes)
		c.queryStateLock.Unlock()
		if err != nil {
			return nil, err
		}

		return res, nil
	}
	c.queryStateLock.Unlock()

	return c.replace(doc, valueBytes)
}

func (c *AttemptContext) replace(doc *GetResult, valueBytes []byte) (resOut *GetResult, errOut error) {
	collection := doc.collection
	id := doc.docID

	waitCh := make(chan struct{}, 1)
	err := c.txn.Replace(coretxns.ReplaceOptions{
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
		}
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Insert will insert a new document, failing if the document already exists.
func (c *AttemptContext) Insert(collection *gocb.Collection, id string, value interface{}) (*GetResult, error) {
	// TODO: Use Transcoder here
	valueBytes, _, err := c.transcoder.Encode(value)
	if err != nil {
		return nil, err
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.insertQueryMode(collection, id, valueBytes)
		c.queryStateLock.Unlock()
		if err != nil {
			return nil, err
		}

		return res, nil
	}
	c.queryStateLock.Unlock()

	return c.insert(collection, id, valueBytes)
}

func (c *AttemptContext) insert(collection *gocb.Collection, id string, valueBytes []byte) (resOut *GetResult, errOut error) {
	a, err := collection.Bucket().Internal().IORouter()
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
		}
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Remove will delete a document.
func (c *AttemptContext) Remove(doc *GetResult) error {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		err := c.removeQueryMode(doc)
		c.queryStateLock.Unlock()
		return err
	}
	c.queryStateLock.Unlock()

	return c.remove(doc)
}

func (c *AttemptContext) remove(doc *GetResult) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.Remove(coretxns.RemoveOptions{
		Document: doc.coreRes,
	}, func(res *coretxns.GetResult, err error) {
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Commit will attempt to commit the transaction in its entirety.
func (c *AttemptContext) Commit() error {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		err := c.commitQueryMode()
		c.queryStateLock.Unlock()
		return err
	}
	c.queryStateLock.Unlock()

	return c.commit()
}

func (c *AttemptContext) commit() (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.Commit(func(err error) {
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh
	return
}

// Rollback will undo all changes related to a transaction.
func (c *AttemptContext) Rollback() error {
	c.queryStateLock.Lock()
	c.userInvokedRollback = true
	if c.queryModeLocked() {
		err := c.rollbackQueryMode()
		c.queryStateLock.Unlock()
		return err
	}
	c.queryStateLock.Unlock()

	return c.rollback()
}

func (c *AttemptContext) rollback() (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.Rollback(func(err error) {
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
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
