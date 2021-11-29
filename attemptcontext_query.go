package transactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbase/gocbcore-transactions"
	"time"
)

func (c *AttemptContext) Query(statement string, options *QueryOptions) (*QueryResult, error) {
	var opts QueryOptions
	if options != nil {
		opts = *options
	}
	c.queryStateLock.Lock()
	res, err := c.query(statement, opts, false)
	c.queryStateLock.Unlock()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *AttemptContext) query(statement string, options QueryOptions, tximplicit bool) (*QueryResult, error) {
	sdkOpts := options.toSDKOptions()
	return c.queryWrapper(options.Scope, statement, sdkOpts, "query", false, true,
		nil, tximplicit)
}

func (c *AttemptContext) queryModeLocked() bool {
	return c.queryState != nil
}

func (c *AttemptContext) queryMode() bool {
	c.queryStateLock.Lock()
	queryMode := c.queryState != nil
	c.queryStateLock.Unlock()

	return queryMode
}

func (c *AttemptContext) getQueryMode(collection *gocb.Collection, id string) (*GetResult, error) {
	txdata := map[string]interface{}{
		"kv": true,
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		// TODO: should we be wrapping this? It really shouldn't happen...
		return nil, err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}
		// Note: We don't return nil here if the error is doc not found (which is what the spec says to do) instead we
		// pick up that error in GetOptional if required.
		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry: true,
			ErrorCause:     err,
			Reason:         coretxns.ErrorReasonTransactionFailed,
		})
	}

	res, err := c.queryWrapper(nil, "EXECUTE __get", gocb.QueryOptions{
		PositionalParameters: []interface{}{c.keyspace(collection), id},
		Adhoc:                true,
	}, "queryKvGet", false, true, b, false)
	if err != nil {
		return nil, handleErr(err)
	}

	type getQueryResult struct {
		Scas    string          `json:"scas"`
		Doc     json.RawMessage `json:"doc"`
		TxnMeta json.RawMessage `json:"txnMeta,omitempty"`
	}

	var row getQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(err)
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &GetResult{
		collection: collection,
		docID:      id,

		transcoder: gocb.NewJSONTranscoder(),
		flags:      2 << 24,

		txnMeta: row.TxnMeta,

		coreRes: &coretxns.GetResult{
			Value: row.Doc,
			Cas:   cas,
		},
	}, nil
}

func (c *AttemptContext) replaceQueryMode(doc *GetResult, valueBytes json.RawMessage) (*GetResult, error) {
	txdata := map[string]interface{}{
		"kv":   true,
		"scas": toScas(doc.coreRes.Cas),
	}

	if len(doc.txnMeta) > 0 {
		txdata["txnMeta"] = doc.txnMeta
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return nil, err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return c.operationFailed(queryOperationFailedDef{
				ErrorCause:      err,
				Reason:          coretxns.ErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      coretxns.ErrorClassFailDocNotFound,
			})
		} else if errors.Is(err, ErrCasMismatch) {
			return c.operationFailed(queryOperationFailedDef{
				ErrorCause:      err,
				Reason:          coretxns.ErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      coretxns.ErrorClassFailCasMismatch,
			})
		}

		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:  true,
			ErrorCause:      err,
			Reason:          coretxns.ErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	params := []interface{}{c.keyspace(doc.collection), doc.docID, valueBytes, json.RawMessage("{}")}

	res, err := c.queryWrapper(nil, "EXECUTE __update", gocb.QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvReplace", false, true, b, false)
	if err != nil {
		return nil, handleErr(err)
	}

	type replaceQueryResult struct {
		Scas string          `json:"scas"`
		Doc  json.RawMessage `json:"doc"`
	}

	var row replaceQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(c.queryMaybeTranslateToTransactionsError(err))
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &GetResult{
		collection: doc.collection,
		docID:      doc.docID,

		transcoder: gocb.NewJSONTranscoder(),
		flags:      2 << 24,

		coreRes: &coretxns.GetResult{
			Value: row.Doc,
			Cas:   cas,
		},
	}, nil
}

func (c *AttemptContext) insertQueryMode(collection *gocb.Collection, id string, valueBytes json.RawMessage) (*GetResult, error) {
	txdata := map[string]interface{}{
		"kv": true,
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return nil, &TransactionOperationFailedError{
			errorCause: err,
		}
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}
		if errors.Is(err, ErrDocumentAlreadyExists) {
			return err
		}

		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:  true,
			ErrorCause:      err,
			Reason:          coretxns.ErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	params := []interface{}{c.keyspace(collection), id, valueBytes, json.RawMessage("{}")}

	res, err := c.queryWrapper(nil, "EXECUTE __insert", gocb.QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvInsert", false, true, b, false)
	if err != nil {
		return nil, handleErr(err)
	}

	type insertQueryResult struct {
		Scas string `json:"scas"`
	}

	var row insertQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(c.queryMaybeTranslateToTransactionsError(err))
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &GetResult{
		collection: collection,
		docID:      id,

		transcoder: gocb.NewJSONTranscoder(),
		flags:      2 << 24,

		coreRes: &coretxns.GetResult{
			Value: valueBytes,
			Cas:   cas,
		},
	}, nil
}

func (c *AttemptContext) removeQueryMode(doc *GetResult) error {
	txdata := map[string]interface{}{
		"kv":   true,
		"scas": toScas(doc.coreRes.Cas),
	}

	if len(doc.txnMeta) > 0 {
		txdata["txnMeta"] = doc.txnMeta
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return c.operationFailed(queryOperationFailedDef{
				ErrorCause:      err,
				Reason:          coretxns.ErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      coretxns.ErrorClassFailDocNotFound,
			})
		} else if errors.Is(err, ErrCasMismatch) {
			return c.operationFailed(queryOperationFailedDef{
				ErrorCause:      err,
				Reason:          coretxns.ErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      coretxns.ErrorClassFailCasMismatch,
			})
		}

		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:  true,
			ErrorCause:      err,
			Reason:          coretxns.ErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	params := []interface{}{c.keyspace(doc.collection), doc.docID, json.RawMessage("{}")}

	res, err := c.queryWrapper(nil, "EXECUTE __delete", gocb.QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvRemove", false, true, b, false)
	if err != nil {
		return handleErr(err)
	}

	if err := c.maybeExtractErrorFromQueryMutation(res); err != nil {
		return handleErr(err)
	}

	return nil
}

func (c *AttemptContext) commitQueryMode() error {
	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrAttemptExpired) {
			return c.operationFailed(queryOperationFailedDef{
				ErrorCause:        err,
				Reason:            coretxns.ErrorReasonTransactionCommitAmbiguous,
				ShouldNotRollback: true,
				ShouldNotRetry:    true,
				ErrorClass:        coretxns.ErrorClassFailExpiry,
			})
		}

		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ErrorCause:        err,
			Reason:            coretxns.ErrorReasonTransactionFailed,
		})
	}

	res, err := c.queryWrapper(nil, "COMMIT", gocb.QueryOptions{
		Adhoc: true,
	}, "queryCommit", false, true, nil, false)
	c.txn.UpdateState(coretxns.UpdateStateOptions{
		ShouldNotCommit: true,
	})
	if err != nil {
		return handleErr(err)
	}

	if err := c.maybeExtractErrorFromQueryMutation(res); err != nil {
		return handleErr(err)
	}

	c.txn.UpdateState(coretxns.UpdateStateOptions{
		ShouldNotRollback: true,
		ShouldNotRetry:    true,
		State:             coretxns.AttemptStateCompleted,
	})

	return nil
}

func (c *AttemptContext) rollbackQueryMode() error {
	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrAttemptNotFoundOnQuery) {
			return nil
		}

		return c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ErrorCause:        err,
			Reason:            coretxns.ErrorReasonTransactionFailed,
			ShouldNotCommit:   true,
		})
	}

	res, err := c.queryWrapper(nil, "ROLLBACK", gocb.QueryOptions{
		Adhoc: true,
	}, "queryRollback", false, false, nil, false)
	c.txn.UpdateState(coretxns.UpdateStateOptions{
		ShouldNotRollback: true,
		ShouldNotCommit:   true,
	})
	if err != nil {
		return handleErr(err)
	}

	if err := c.maybeExtractErrorFromQueryMutation(res); err != nil {
		return handleErr(err)
	}

	c.txn.UpdateState(coretxns.UpdateStateOptions{
		State: coretxns.AttemptStateRolledBack,
	})

	return nil
}

type jsonTransactionOperationFailed struct {
	Cause    interface{} `json:"cause"`
	Retry    bool        `json:"retry"`
	Rollback bool        `json:"rollback"`
	Raise    string      `json:"raise"`
}

type jsonQueryTransactionOperationFailedCause struct {
	Cause   *jsonTransactionOperationFailed `json:"cause"`
	Code    uint32                          `json:"code"`
	Message string                          `json:"message"`
}

func durabilityLevelToQueryString(level coretxns.DurabilityLevel) string {
	switch level {
	case coretxns.DurabilityLevelUnknown:
		return "unset"
	case coretxns.DurabilityLevelNone:
		return "none"
	case coretxns.DurabilityLevelMajority:
		return "majority"
	case coretxns.DurabilityLevelMajorityAndPersistToActive:
		return "majorityAndPersistActive"
	case coretxns.DurabilityLevelPersistToMajority:
		return "persistToMajority"
	}
	return ""
}

func (c *AttemptContext) maybeExtractErrorFromQueryMutation(res *QueryResult) error {
	for res.Next() {
	}

	if err := res.Err(); err != nil {
		return c.queryMaybeTranslateToTransactionsError(err)
	}

	return nil
}

func (c *AttemptContext) queryWrapper(scope *gocb.Scope, statement string, options gocb.QueryOptions, hookPoint string,
	isBeginWork bool, existingErrorCheck bool, txData []byte, txImplicit bool) (*QueryResult, error) {

	var target string
	if !isBeginWork {
		if !c.queryModeLocked() {
			// This is quite a big lock but we can't put the context into "query mode" until we know that begin work was
			// successful. We also can't allow any further ops to happen until we know if we're in "query mode" or not.

			// queryBeginWork implicitly performs an existingErrorCheck and the call into Serialize on the coretxns side
			// will return an error if there have been any previously failed operations.
			if err := c.queryBeginWork(); err != nil {
				return nil, err
			}
		}

		// If we've got here then queryState cannot be nil.
		target = c.queryState.queryTarget

		if !c.txn.CanCommit() && !c.txn.ShouldRollback() {
			return nil, c.operationFailed(queryOperationFailedDef{
				ShouldNotRetry:    true,
				Reason:            coretxns.ErrorReasonTransactionFailed,
				ErrorCause:        ErrOther,
				ErrorClass:        coretxns.ErrorClassFailOther,
				ShouldNotRollback: true,
			})
		}
	}

	if existingErrorCheck {
		if !c.txn.CanCommit() {
			return nil, c.operationFailed(queryOperationFailedDef{
				ShouldNotRetry: true,
				Reason:         coretxns.ErrorReasonTransactionFailed,
				ErrorCause:     ErrPreviousOperationFailed,
				ErrorClass:     coretxns.ErrorClassFailOther,
			})
		}
	}

	expired, err := c.hooks.HasExpiredClientSideHook(*c, hookPoint, statement)
	if err != nil {
		// This isn't meant to happen...
		return nil, &TransactionOperationFailedError{
			errorCause: err,
		}
	}
	cfg := c.txn.Config()
	if cfg.ExpirationTime < 10*time.Millisecond || expired {
		return nil, c.operationFailed(queryOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            coretxns.ErrorReasonTransactionExpired,
			ErrorCause:        ErrAttemptExpired,
			ErrorClass:        coretxns.ErrorClassFailExpiry,
		})
	}

	options.Metrics = true
	options.Internal.Endpoint = target
	if options.Raw == nil {
		options.Raw = make(map[string]interface{})
	}
	if !isBeginWork && !txImplicit {
		options.Raw["txid"] = c.txn.Attempt().ID
	}

	if len(txData) > 0 {
		options.Raw["txdata"] = json.RawMessage(txData)
	}
	if txImplicit {
		options.Raw["tximplicit"] = true
	}
	options.Timeout = cfg.ExpirationTime + cfg.KeyValueTimeout + (1 * time.Second) // TODO: add timeout value here

	err = c.hooks.BeforeQuery(*c, statement)
	if err != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(err)
	}

	var result *gocb.QueryResult
	var queryErr error
	if scope == nil {
		result, queryErr = c.cluster.Query(statement, &options)
	} else {
		result, queryErr = scope.Query(statement, &options)
	}
	if queryErr != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(queryErr)
	}

	err = c.hooks.AfterQuery(*c, statement)
	if err != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(err)
	}

	return newQueryResult(result, c), nil
}

func (c *AttemptContext) queryBeginWork() (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.SerializeAttempt(func(txdata []byte, err error) {
		if err != nil {
			var coreErr *coretxns.TransactionOperationFailedError
			if errors.As(err, &coreErr) {
				// Note that we purposely do not use operationFailed here, we haven't moved into query mode yet.
				// State will continue to be controlled from the gocbcore side.
				errOut = &TransactionOperationFailedError{
					shouldRetry:       coreErr.Retry(),
					shouldNotRollback: !coreErr.Rollback(),
					errorCause:        coreErr,
					shouldRaise:       coreErr.ToRaise(),
					errorClass:        coreErr.ErrorClass(),
				}
			} else {
				errOut = err
			}
			waitCh <- struct{}{}
			return
		}

		c.queryState = &queryState{}

		cfg := c.txn.Config()
		raw := make(map[string]interface{})
		raw["durability_level"] = durabilityLevelToQueryString(cfg.DurabilityLevel)
		raw["txtimeout"] = fmt.Sprintf("%dms", cfg.ExpirationTime.Milliseconds())
		if cfg.CustomATRLocation.Agent != nil {
			// Agent being non nil signifies that this was set.
			raw["atrcollection"] = fmt.Sprintf(
				"%s.%s.%s",
				cfg.CustomATRLocation.Agent.BucketName(),
				cfg.CustomATRLocation.ScopeName,
				cfg.CustomATRLocation.CollectionName,
			)
		}

		res, err := c.queryWrapper(nil, "BEGIN WORK", gocb.QueryOptions{
			ScanConsistency: gocb.QueryScanConsistency(c.queryConfig.ScanConsistency),
			Raw:             raw,
			Adhoc:           true,
		}, "queryBeginWork", true, false,
			txdata, false)
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		for res.Next() {
		}
		if err := res.Err(); err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		c.queryState.queryTarget = res.wrapped.Internal().Endpoint()

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh

	return
}

func (c *AttemptContext) keyspace(collection *gocb.Collection) string {
	return fmt.Sprintf("default:`%s`.`%s`.`%s`", collection.Bucket().Name(), collection.ScopeName(), collection.Name())
}
