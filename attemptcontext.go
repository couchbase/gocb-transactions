package transactions

import (
	"errors"
	"time"

	gocb "github.com/couchbase/gocb/v2"
)

type attemptState int

const (
	attemptStateNothingWritten = attemptState(1)
	attemptStatePending        = attemptState(2)
	attemptStateCommitted      = attemptState(3)
	attemptStateCompleted      = attemptState(4)
	attemptStateAborted        = attemptState(5)
	attemptStateRolledBack     = attemptState(6)
)

// AttemptContext represents a single attempt to execute a transaction.
type AttemptContext struct {
	transactionID       string
	id                  string
	state               attemptState
	stagedMutations     []interface{}
	finalMutationTokens gocb.MutationState
	atrID               int
	atrCollection       *gocb.Collection
	expiryOvertimeMode  bool
	expiryTime          time.Time
}

func (c *AttemptContext) checkDone() error {
	if c.state != attemptStateNothingWritten && c.state != attemptStatePending {
		return ErrOther
	}

	return nil
}

func (c *AttemptContext) setATRPending(collection *gocb.Collection, docID string) error {
	if c.state != attemptStateNothingWritten {
		return nil
	}

	if time.Now().After(c.expiryTime) {
		return ErrAttemptExpired
	}

	atrID := int(cbcVbMap(docID, 1024))
	atrDocID := atrIDList[atrID]

	c.atrID = atrID
	c.atrCollection = collection

	atrFieldSpec := func(fieldName string, data interface{}) gocb.MutateInSpec {
		return gocb.UpsertSpec("attempts."+c.id+"."+fieldName, data, &gocb.UpsertSpecOptions{
			CreatePath: true,
		})
	}

	_, err := c.atrCollection.MutateIn(atrDocID, []gocb.MutateInSpec{
		atrFieldSpec("tid", c.transactionID),
		atrFieldSpec("st", "PENDING"),
		atrFieldSpec("tst", gocb.MutationMacroCAS),
		atrFieldSpec("exp", c.expiryTime.Sub(time.Now())),
	}, &gocb.MutateInOptions{
		DurabilityLevel: gocb.DurabilityLevel(0),
		StoreSemantic:   gocb.StoreSemanticsUpsert,
	})
	if err != nil {
		return ErrHard
	}

	c.state = attemptStatePending

	return nil
}

func (c *AttemptContext) checkWriteWriteConflict() error {
	return nil
}

// GetOptional will attempt to fetch a document, and return nil if it does not exist.
func (c *AttemptContext) GetOptional(collection *gocb.Collection, id string) (*GetResult, error) {
	res, err := c.GetOptional(collection, id)
	if err == ErrDocNotFound {
		return nil, nil
	}
	return res, err
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *AttemptContext) Get(collection *gocb.Collection, id string) (*GetResult, error) {
	err := c.checkDone()
	if err != nil {
		return nil, err
	}

	return nil, errors.New("not implemented")
}

// Replace will replace the contents of a document, failing if the document does not already exist.
func (c *AttemptContext) Replace(doc *GetResult, value interface{}) (*GetResult, error) {
	collection := doc.collection
	id := doc.docID

	err := c.checkDone()
	if err != nil {
		return nil, err
	}

	if time.Now().After(c.expiryTime) {
		return nil, ErrAttemptExpired
	}

	err = c.checkWriteWriteConflict()
	if err != nil {
		return nil, err
	}

	err = c.setATRPending(collection, id)
	if err != nil {
		return nil, err
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = c.transactionID
	txnMeta.ID.Attempt = c.id
	txnMeta.ATR.CollectionName = c.atrCollection.Name()
	txnMeta.ATR.BucketName = "lol"
	txnMeta.ATR.DocID = atrIDList[c.atrID]
	txnMeta.Operation.Type = "replace"
	txnMeta.Operation.Staged = value

	res, err := collection.MutateIn(id, []gocb.MutateInSpec{
		gocb.InsertSpec("txn", txnMeta, &gocb.InsertSpecOptions{
			CreatePath: true,
			IsXattr:    true,
		}),
	}, &gocb.MutateInOptions{
		DurabilityLevel: gocb.DurabilityLevel(0),
		StoreSemantic:   gocb.StoreSemanticsReplace,
	})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		collection: collection,
		docID:      id,
		cas:        res.Cas(),
	}, nil
}

// Insert will insert a new document, failing if the document already exists.
func (c *AttemptContext) Insert(collection *gocb.Collection, id string, value interface{}) (*GetResult, error) {
	err := c.checkDone()
	if err != nil {
		return nil, err
	}

	if time.Now().After(c.expiryTime) {
		return nil, ErrAttemptExpired
	}

	err = c.setATRPending(collection, id)
	if err != nil {
		return nil, err
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = c.transactionID
	txnMeta.ID.Attempt = c.id
	txnMeta.ATR.CollectionName = c.atrCollection.Name()
	txnMeta.ATR.BucketName = ""
	txnMeta.ATR.DocID = atrIDList[c.atrID]
	txnMeta.Operation.Type = "insert"
	txnMeta.Operation.Staged = value

	res, err := collection.MutateIn(id, []gocb.MutateInSpec{
		gocb.InsertSpec("txn", txnMeta, &gocb.InsertSpecOptions{
			CreatePath: true,
			IsXattr:    true,
		}),
	}, &gocb.MutateInOptions{
		DurabilityLevel: gocb.DurabilityLevel(0),
		StoreSemantic:   gocb.StoreSemanticsInsert,
	})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		collection: collection,
		docID:      id,
		cas:        res.Cas(),
	}, nil
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
