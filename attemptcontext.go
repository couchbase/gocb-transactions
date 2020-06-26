package transactions

import (
	"encoding/json"
	"errors"
	"log"
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

type stagedMutationType int

const (
	stagedMutationInsert  = stagedMutationType(1)
	stagedMutationReplace = stagedMutationType(2)
	stagedMutationRemove  = stagedMutationType(3)
)

type stagedMutation struct {
	OpType     stagedMutationType
	Collection *gocb.Collection
	DocID      string
	Cas        gocb.Cas
	Staged     json.RawMessage
}

// AttemptContext represents a single attempt to execute a transaction.
type AttemptContext struct {
	transactionID       string
	id                  string
	state               attemptState
	stagedMutations     []stagedMutation
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
		atrFieldSpec("st", jsonAtrStatePending),
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

func (c *AttemptContext) setATRCommitted() error {
	if c.state != attemptStatePending {
		return ErrUhOh
	}

	var insMutations []jsonAtrMutation
	var repMutations []jsonAtrMutation
	var remMutations []jsonAtrMutation

	for _, mutation := range c.stagedMutations {
		jsonMutation := jsonAtrMutation{
			BucketName:     "",
			ScopeName:      "",
			CollectionName: mutation.Collection.Name(),
			DocID:          mutation.DocID,
		}

		if mutation.OpType == stagedMutationInsert {
			insMutations = append(insMutations, jsonMutation)
		} else if mutation.OpType == stagedMutationReplace {
			repMutations = append(repMutations, jsonMutation)
		} else if mutation.OpType == stagedMutationRemove {
			remMutations = append(remMutations, jsonMutation)
		} else {
			return ErrOther
		}
	}

	atrID := c.atrID
	atrDocID := atrIDList[atrID]

	atrFieldSpec := func(fieldName string, data interface{}) gocb.MutateInSpec {
		return gocb.UpsertSpec("attempts."+c.id+"."+fieldName, data, &gocb.UpsertSpecOptions{
			CreatePath: true,
		})
	}

	_, err := c.atrCollection.MutateIn(atrDocID, []gocb.MutateInSpec{
		atrFieldSpec("st", jsonAtrStateCommitted),
		atrFieldSpec("tsc", gocb.MutationMacroCAS),
		atrFieldSpec("ins", insMutations),
		atrFieldSpec("rep", repMutations),
		atrFieldSpec("rem", remMutations),
		atrFieldSpec("p", 0),
	}, &gocb.MutateInOptions{
		DurabilityLevel: gocb.DurabilityLevel(0),
		StoreSemantic:   gocb.StoreSemanticsUpsert,
	})
	if err != nil {
		return ErrHard
	}

	c.state = attemptStateCommitted

	return nil
}

func (c *AttemptContext) setATRCompleted() error {
	if c.state != attemptStateCommitted {
		return ErrUhOh
	}

	atrID := c.atrID
	atrDocID := atrIDList[atrID]

	atrFieldSpec := func(fieldName string, data interface{}) gocb.MutateInSpec {
		return gocb.UpsertSpec("attempts."+c.id+"."+fieldName, data, &gocb.UpsertSpecOptions{
			CreatePath: true,
		})
	}

	_, err := c.atrCollection.MutateIn(atrDocID, []gocb.MutateInSpec{
		atrFieldSpec("st", jsonAtrStateCompleted),
		atrFieldSpec("tsco", gocb.MutationMacroCAS),
	}, &gocb.MutateInOptions{
		DurabilityLevel: gocb.DurabilityLevel(0),
		StoreSemantic:   gocb.StoreSemanticsUpsert,
	})
	if err != nil {
		return ErrHard
	}

	return nil
}

func (c *AttemptContext) unstageInsRepMutation(mutation stagedMutation) error {
	if mutation.OpType != stagedMutationInsert && mutation.OpType != stagedMutationReplace {
		return ErrUhOh
	}

	_, err := mutation.Collection.Replace(mutation.DocID, mutation.Staged, &gocb.ReplaceOptions{
		Cas: mutation.Cas,
	})
	return err
}

func (c *AttemptContext) unstageRemMutation(mutation stagedMutation) error {
	if mutation.OpType != stagedMutationRemove {
		return ErrUhOh
	}

	_, err := mutation.Collection.Remove(mutation.DocID, nil)
	return err
}

func (c *AttemptContext) checkWriteWriteConflict() error {
	return nil
}

// GetOptional will attempt to fetch a document, and return nil if it does not exist.
func (c *AttemptContext) GetOptional(collection *gocb.Collection, id string) (*GetResult, error) {
	res, err := c.Get(collection, id)
	if err == ErrDocNotFound {
		return nil, nil
	}
	return res, err
}

func (c *AttemptContext) getStagedMutation(collection *gocb.Collection, id string) *stagedMutation {
	for _, mutation := range c.stagedMutations {
		if mutation.Collection.Name() == collection.Name() && mutation.DocID == id {
			return &mutation
		}
	}

	return nil
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *AttemptContext) Get(collection *gocb.Collection, id string) (*GetResult, error) {
	err := c.checkDone()
	if err != nil {
		return nil, err
	}

	if time.Now().After(c.expiryTime) {
		return nil, ErrAttemptExpired
	}

	existingMutation := c.getStagedMutation(collection, id)
	if existingMutation != nil {
		if existingMutation.OpType == stagedMutationInsert || existingMutation.OpType == stagedMutationReplace {
			return &GetResult{
				collection: collection,
				docID:      id,
				cas:        gocb.Cas(0),
				transcoder: gocb.NewJSONTranscoder(),
				flags:      (2 << 24),
				contents:   existingMutation.Staged,
			}, nil
		} else if existingMutation.OpType == stagedMutationRemove {
			return nil, ErrDocNotFound
		} else {
			return nil, ErrOther
		}
	}

	res, err := collection.LookupIn(id, []gocb.LookupInSpec{
		gocb.GetSpec("$document", &gocb.GetSpecOptions{
			IsXattr: true,
		}),
		gocb.GetSpec("txn", &gocb.GetSpecOptions{
			IsXattr: true,
		}),
		gocb.GetSpec("", &gocb.GetSpecOptions{}),
	}, &gocb.LookupInOptions{})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, ErrDocNotFound
		}

		return nil, err
	}

	var docMeta struct {
		Cas string `json:"CAS"`
	}
	err = res.ContentAt(0, &docMeta)
	if err != nil {
		return nil, err
	}

	var txnMeta *jsonTxnXattr
	var txnMetaVal jsonTxnXattr
	err = res.ContentAt(1, &txnMetaVal)
	if err != nil {
		if !errors.Is(err, gocb.ErrPathNotFound) {
			return nil, err
		}
	} else {
		txnMeta = &txnMetaVal
	}

	var docData json.RawMessage
	err = res.ContentAt(2, &docData)
	if err != nil {
		return nil, err
	}

	if txnMeta != nil {
		// Default to committed in the case that this attempt matches
		// the attempt marker in the transaction meta-data.
		txnAtrState := jsonAtrStateCommitted

		if txnMeta.ID.Attempt != c.id {
			res, err := collection.LookupIn(txnMeta.ATR.DocID, []gocb.LookupInSpec{
				gocb.GetSpec("attempts."+c.id+".st", nil),
			}, &gocb.LookupInOptions{})
			if err != nil {
				if errors.Is(err, gocb.ErrDocumentNotFound) {
					return nil, ErrAtrNotFound
				}

				return nil, err
			}

			err = res.ContentAt(0, &txnAtrState)
			if err != nil {
				if errors.Is(err, gocb.ErrPathNotFound) {
					return nil, ErrAtrEntryNotFound
				}

				return nil, err
			}
		}

		if txnAtrState == jsonAtrStateCommitted {
			if txnMeta.Operation.Type == jsonMutationInsert || txnMeta.Operation.Type == jsonMutationReplace {
				return &GetResult{
					collection: collection,
					docID:      id,
					cas:        res.Cas(),
					transcoder: gocb.NewJSONTranscoder(),
					flags:      (2 << 24),
					contents:   txnMeta.Operation.Staged,
				}, nil
			} else if txnMeta.Operation.Type == jsonMutationRemove {
				return nil, ErrDocNotFound
			} else {
				return nil, ErrOther
			}
		}
	}

	log.Printf("DOC:%v\nTXNPTR:%v\nTXN:%v\nDATA:%v", docMeta, txnMeta, txnMetaVal, docData)

	return &GetResult{
		collection: collection,
		docID:      id,

		cas:        res.Cas(),
		transcoder: gocb.NewJSONTranscoder(),
		flags:      (2 << 24),
		contents:   docData,
	}, nil
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

	// TODO: Use Transcoder here
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = c.transactionID
	txnMeta.ID.Attempt = c.id
	txnMeta.ATR.CollectionName = c.atrCollection.Name()
	txnMeta.ATR.BucketName = ""
	txnMeta.ATR.DocID = atrIDList[c.atrID]
	txnMeta.Operation.Type = jsonMutationReplace
	txnMeta.Operation.Staged = valueBytes

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

	c.stagedMutations = append(c.stagedMutations, stagedMutation{
		OpType:     stagedMutationReplace,
		Collection: collection,
		DocID:      id,
		Cas:        res.Cas(),
		Staged:     valueBytes,
	})

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

	// TODO: Use Transcoder here
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = c.transactionID
	txnMeta.ID.Attempt = c.id
	txnMeta.ATR.CollectionName = c.atrCollection.Name()
	txnMeta.ATR.BucketName = ""
	txnMeta.ATR.DocID = atrIDList[c.atrID]
	txnMeta.Operation.Type = jsonMutationInsert
	txnMeta.Operation.Staged = valueBytes

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

	c.stagedMutations = append(c.stagedMutations, stagedMutation{
		OpType:     stagedMutationInsert,
		Collection: collection,
		DocID:      id,
		Cas:        res.Cas(),
		Staged:     valueBytes,
	})

	return &GetResult{
		collection: collection,
		docID:      id,
		cas:        res.Cas(),
	}, nil
}

// Remove will delete a document.
func (c *AttemptContext) Remove(doc *GetResult) error {
	collection := doc.collection
	id := doc.docID

	err := c.checkDone()
	if err != nil {
		return err
	}

	if time.Now().After(c.expiryTime) {
		return ErrAttemptExpired
	}

	err = c.checkWriteWriteConflict()
	if err != nil {
		return err
	}

	err = c.setATRPending(collection, id)
	if err != nil {
		return err
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = c.transactionID
	txnMeta.ID.Attempt = c.id
	txnMeta.ATR.CollectionName = c.atrCollection.Name()
	txnMeta.ATR.BucketName = ""
	txnMeta.ATR.DocID = atrIDList[c.atrID]
	txnMeta.Operation.Type = jsonMutationRemove
	txnMeta.Operation.Staged = nil

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
		return err
	}

	c.stagedMutations = append(c.stagedMutations, stagedMutation{
		OpType:     stagedMutationRemove,
		Collection: collection,
		DocID:      id,
		Cas:        res.Cas(),
		Staged:     nil,
	})

	return nil
}

// Commit will attempt to commit the transaction in its entirety.
func (c *AttemptContext) Commit() error {
	if time.Now().After(c.expiryTime) {
		return ErrAttemptExpired
	}

	err := c.setATRCommitted()
	if err != nil {
		return err
	}

	for _, mutation := range c.stagedMutations {
		if mutation.OpType == stagedMutationInsert || mutation.OpType == stagedMutationReplace {
			err := c.unstageInsRepMutation(mutation)
			if err != nil {
				return err
			}
		} else if mutation.OpType == stagedMutationRemove {
			err := c.unstageRemMutation(mutation)
			if err != nil {
				return err
			}
		} else {
			return ErrUhOh
		}
	}

	err = c.setATRCompleted()
	if err != nil {
		return err
	}

	return nil
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
