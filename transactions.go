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
	"github.com/couchbase/gocbcore/v10"
	"log"
	"math"
	"sync"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbase/gocbcore-transactions"
)

type AttemptFunc func(*AttemptContext) error

type Transactions struct {
	config     Config
	cluster    *gocb.Cluster
	transcoder gocb.Transcoder

	txns                *coretxns.Manager
	hooksWrapper        hooksWrapper
	cleanupHooksWrapper cleanupHooksWrapper
}

// Init will initialize the transactions library and return a Transactions
// object which can be used to perform transactions.
func Init(cluster *gocb.Cluster, config *Config) (*Transactions, error) {
	if config == nil {
		config = &Config{
			DurabilityLevel: DurabilityLevelMajority,
			QueryConfig: QueryConfig{
				ScanConsistency: gocb.QueryScanConsistencyRequestPlus,
			},
		}
	}
	// TODO we're gonna have to get this from gocb somehow.
	if config.KeyValueTimeout == 0 {
		config.KeyValueTimeout = 10000 * time.Millisecond
	}
	if config.QueryConfig.ScanConsistency == 0 {
		config.QueryConfig.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
	}

	var hooksWrapper hooksWrapper
	if config.Internal.Hooks == nil {
		hooksWrapper = &noopHooksWrapper{
			DefaultHooks: coretxns.DefaultHooks{},
			hooks:        defaultHooks{},
		}
	} else {
		hooksWrapper = &coreTxnsHooksWrapper{
			hooks: config.Internal.Hooks,
		}
	}

	var cleanupHooksWrapper cleanupHooksWrapper
	if config.Internal.CleanupHooks == nil {
		cleanupHooksWrapper = &noopCleanupHooksWrapper{
			DefaultCleanupHooks: coretxns.DefaultCleanupHooks{},
		}
	} else {
		cleanupHooksWrapper = &coreTxnsCleanupHooksWrapper{
			CleanupHooks: config.Internal.CleanupHooks,
		}
	}

	var clientRecordHooksWrapper clientRecordHooksWrapper
	if config.Internal.ClientRecordHooks == nil {
		clientRecordHooksWrapper = &noopClientRecordHooksWrapper{
			DefaultCleanupHooks:      coretxns.DefaultCleanupHooks{},
			DefaultClientRecordHooks: coretxns.DefaultClientRecordHooks{},
		}
	} else {
		clientRecordHooksWrapper = &coreTxnsClientRecordHooksWrapper{
			coreTxnsCleanupHooksWrapper: coreTxnsCleanupHooksWrapper{
				CleanupHooks: config.Internal.CleanupHooks,
			},
			ClientRecordHooks: config.Internal.ClientRecordHooks,
		}
	}

	t := &Transactions{
		cluster:             cluster,
		config:              *config,
		transcoder:          gocb.NewJSONTranscoder(),
		hooksWrapper:        hooksWrapper,
		cleanupHooksWrapper: cleanupHooksWrapper,
	}

	atrLocation := coretxns.ATRLocation{}
	if config.MetadataCollection != nil {
		customATRAgent, err := config.MetadataCollection.Bucket().Internal().IORouter()
		if err != nil {
			return nil, err
		}
		atrLocation.Agent = customATRAgent
		atrLocation.CollectionName = config.MetadataCollection.Name()
		atrLocation.ScopeName = config.MetadataCollection.ScopeName()
	}

	corecfg := &coretxns.Config{}
	corecfg.DurabilityLevel = coretxns.DurabilityLevel(config.DurabilityLevel)
	corecfg.KeyValueTimeout = config.KeyValueTimeout
	corecfg.BucketAgentProvider = t.agentProvider
	corecfg.LostCleanupATRLocationProvider = t.atrLocationsProvider
	corecfg.CleanupClientAttempts = config.CleanupClientAttempts
	corecfg.CleanupQueueSize = config.CleanupQueueSize
	corecfg.ExpirationTime = config.ExpirationTime
	corecfg.CleanupWindow = config.CleanupWindow
	corecfg.CleanupLostAttempts = config.CleanupLostAttempts
	corecfg.CustomATRLocation = atrLocation
	corecfg.Internal.Hooks = hooksWrapper
	corecfg.Internal.CleanUpHooks = cleanupHooksWrapper
	corecfg.Internal.ClientRecordHooks = clientRecordHooksWrapper
	corecfg.Internal.NumATRs = config.Internal.NumATRs

	txns, err := coretxns.Init(corecfg)
	if err != nil {
		return nil, err
	}

	t.txns = txns
	return t, nil
}

// Run runs a lambda to perform a number of operations as part of a
// singular transaction.
func (t *Transactions) Run(logicFn AttemptFunc, perConfig *PerTransactionConfig) (*Result, error) {
	if perConfig == nil {
		perConfig = &PerTransactionConfig{
			DurabilityLevel: t.config.DurabilityLevel,
			ExpirationTime:  t.config.ExpirationTime,
		}
	}

	scanConsistency := perConfig.QueryConfig.ScanConsistency
	if scanConsistency == 0 {
		scanConsistency = t.config.QueryConfig.ScanConsistency
	}

	// TODO: fill in the rest of this config
	txn, err := t.txns.BeginTransaction(&coretxns.PerTransactionConfig{
		DurabilityLevel: coretxns.DurabilityLevel(perConfig.DurabilityLevel),
		ExpirationTime:  perConfig.ExpirationTime,
	})
	if err != nil {
		return nil, err
	}

	retries := 0
	backoffCalc := func() time.Duration {
		var max float64 = 100000000 // 100 Milliseconds
		var min float64 = 1000000   // 1 Millisecond
		retries++
		backoff := min * (math.Pow(2, float64(retries)))

		if backoff > max {
			backoff = max
		}
		if backoff < min {
			backoff = min
		}

		return time.Duration(backoff)
	}

	for {
		err = txn.NewAttempt()
		if err != nil {
			return nil, err
		}

		attempt := AttemptContext{
			txn:            txn,
			transcoder:     t.transcoder,
			hooks:          t.hooksWrapper.Hooks(),
			cluster:        t.cluster,
			queryStateLock: new(sync.Mutex),
			queryConfig: PerTransactionQueryConfig{
				ScanConsistency: scanConsistency,
			},
		}

		if t.hooksWrapper != nil {
			t.hooksWrapper.SetAttemptContext(attempt)
		}

		lambdaErr := logicFn(&attempt)

		// The Rollback method when invoked by the user will potentially return nil with
		// no errors indicating that a retry should not be performed.  In gocbcoretxn, this
		// responsibility lies with the caller, so we need to wrap it in gocbtxn.
		var userRolledback bool
		if lambdaErr == nil && attempt.userInvokedRollback {
			// lambdaErr = errors.New("user initiated rollback")
			userRolledback = true
		}

		var finalErr error
		// Only attempt commit or rollback if the user didn't initiate rollback, if they did then we know we can't
		// commit or rollback anyway.
		if userRolledback {
			finalErr = lambdaErr
		} else {
			if lambdaErr == nil {
				if attempt.canCommit() {
					finalErr = attempt.Commit()
				} else if attempt.shouldRollback() {
					finalErr = attempt.Rollback()
				}
			} else {
				finalErr = lambdaErr

				if attempt.shouldRollback() {
					rollbackErr := attempt.Rollback()
					if rollbackErr != nil {
						log.Printf("rollback after error failed: %s", rollbackErr)
					}
				}
			}
		}

		var finalErrCause error
		var wasUserError bool
		if finalErr == nil && userRolledback {
			// nasty
			wasUserError = true
		} else if finalErr != nil {
			var txnErr *TransactionOperationFailedError
			if errors.As(finalErr, &txnErr) {
				finalErrCause = txnErr.Unwrap()
			} else {
				wasUserError = true
			}
		}

		a := attempt.attempt()

		if lambdaErr == nil {
			a.PreExpiryAutoRollback = false
		}

		if !a.Expired && attempt.shouldRetry() && !wasUserError {
			log.Printf("retrying lambda after backoff")
			time.Sleep(backoffCalc())
			continue
		}

		switch a.State {
		case AttemptStateNothingWritten:
			fallthrough
		case AttemptStatePending:
			fallthrough
		case AttemptStateAborted:
			fallthrough
		case AttemptStateRolledBack:
			if attempt.userInvokedRollback && finalErr == nil {
				unstagingComplete := a.State == AttemptStateCompleted

				return &Result{
					TransactionID:     txn.ID(),
					UnstagingComplete: unstagingComplete,
				}, nil
			}

			if a.Expired && !a.PreExpiryAutoRollback && !wasUserError {
				return nil, &TransactionExpiredError{
					result: &Result{
						TransactionID:     txn.ID(),
						UnstagingComplete: false,
					},
				}
			}

			return nil, &TransactionFailedError{
				cause: finalErrCause,
				result: &Result{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
				},
			}
		case AttemptStateCommitting:
			return nil, &TransactionCommitAmbiguousError{
				cause: finalErrCause,
				result: &Result{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
				},
			}
		case AttemptStateCommitted:
			fallthrough
		case AttemptStateCompleted:
			unstagingComplete := a.State == AttemptStateCompleted

			return &Result{
				TransactionID:     txn.ID(),
				UnstagingComplete: unstagingComplete,
			}, nil
		default:
			return nil, errors.New("invalid final transaction state")
		}
	}
}

// Commit will commit a previously prepared and serialized transaction.
func (t *Transactions) Commit(serialized SerializedContext, perConfig *PerTransactionConfig) error {
	return errors.New("not implemented")
}

// Rollback will commit a previously prepared and serialized transaction.
func (t *Transactions) Rollback(serialized SerializedContext, perConfig *PerTransactionConfig) error {
	return errors.New("not implemented")
}

//
func (t *Transactions) Query(statement string, options *SingleQueryTransactionConfig) (*SingleQueryTransactionResult, error) {
	if options == nil {
		options = &SingleQueryTransactionConfig{}
	}
	var qResult SingleQueryTransactionResult
	tResult, err := t.Run(func(context *AttemptContext) error {
		res, err := context.query(statement, options.QueryOptions, true)
		if err != nil {
			return err
		}

		qResult.wrapped = res
		return nil
	}, &PerTransactionConfig{
		DurabilityLevel: options.DurabilityLevel,
		ExpirationTime:  options.ExpirationTime,
	})
	if err != nil {
		return nil, err
	}

	qResult.unstagingComplete = tResult.UnstagingComplete

	return &qResult, nil
}

// Close will shut down this Transactions object, shutting down all
// background tasks associated with it.
func (t *Transactions) Close() error {
	return t.txns.Close()
}

func (t *Transactions) agentProvider(bucketName string) (*gocbcore.Agent, string, error) {
	b := t.cluster.Bucket(bucketName)
	agent, err := b.Internal().IORouter()
	if err != nil {
		return nil, "", err
	}

	return agent, "", err
}

func (t *Transactions) atrLocationsProvider() ([]coretxns.LostATRLocation, error) {
	meta := t.config.MetadataCollection
	if meta == nil {
		b, err := t.cluster.Buckets().GetAllBuckets(&gocb.GetAllBucketsOptions{
			Timeout: t.config.KeyValueTimeout,
		})
		if err != nil {
			return nil, err
		}

		var names []coretxns.LostATRLocation
		for name := range b {
			names = append(names, coretxns.LostATRLocation{
				BucketName:     name,
				ScopeName:      "",
				CollectionName: "",
			})
		}
		return names, nil
	} else {
		return []coretxns.LostATRLocation{
			{
				BucketName:     meta.Bucket().Name(),
				ScopeName:      meta.ScopeName(),
				CollectionName: meta.Name(),
			},
		}, nil
	}
}

// TransactionsInternal exposes internal methods that are useful for testing and/or
// other forms of internal use.
type TransactionsInternal struct {
	parent *Transactions
}

// Internal returns an TransactionsInternal object which can be used for specialized
// internal use cases.
func (t *Transactions) Internal() *TransactionsInternal {
	return &TransactionsInternal{
		parent: t,
	}
}

// ForceCleanupQueue forces the transactions client cleanup queue to drain without waiting for expirations.
func (t *TransactionsInternal) ForceCleanupQueue() []CleanupAttempt {
	waitCh := make(chan []coretxns.CleanupAttempt, 1)
	t.parent.txns.Internal().ForceCleanupQueue(func(attempts []coretxns.CleanupAttempt) {
		waitCh <- attempts
	})
	coreAttempts := <-waitCh

	var attempts []CleanupAttempt
	for _, attempt := range coreAttempts {
		attempts = append(attempts, cleanupAttemptFromCore(attempt))
	}

	return attempts
}

// CleanupQueueLength returns the current length of the client cleanup queue.
func (t *TransactionsInternal) CleanupQueueLength() int32 {
	return t.parent.txns.Internal().CleanupQueueLength()
}

// ClientCleanupEnabled returns whether the client cleanup process is enabled.
func (t *TransactionsInternal) ClientCleanupEnabled() bool {
	return t.parent.txns.Config().CleanupClientAttempts
}
