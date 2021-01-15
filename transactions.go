package transactions

import (
	"errors"
	"log"
	"math"
	"time"

	"github.com/couchbase/gocbcore/v9"

	gocb "github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
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
		}
	}
	// TODO we're gonna have to get this from gocb somehow.
	if config.KeyValueTimeout == 0 {
		config.KeyValueTimeout = 10000 * time.Millisecond
	}

	var hooksWrapper hooksWrapper
	if config.Internal.Hooks == nil {
		hooksWrapper = &noopHooksWrapper{
			DefaultHooks: coretxns.DefaultHooks{},
		}
	} else {
		hooksWrapper = &coreTxnsHooksWrapper{
			Hooks: config.Internal.Hooks,
		}
	}

	var cleanupHooksWrapper cleanupHooksWrapper
	if config.Internal.Hooks == nil {
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

	corecfg := &coretxns.Config{}
	corecfg.DurabilityLevel = coretxns.DurabilityLevel(config.DurabilityLevel)
	corecfg.KeyValueTimeout = config.KeyValueTimeout
	corecfg.BucketAgentProvider = t.agentProvider
	corecfg.BucketListProvider = t.bucketListProvider
	corecfg.CleanupClientAttempts = config.CleanupClientAttempts
	corecfg.CleanupQueueSize = config.CleanupQueueSize
	corecfg.ExpirationTime = config.ExpirationTime
	corecfg.CleanupWindow = config.CleanupWindow
	corecfg.CleanupLostAttempts = config.CleanupLostAttempts
	corecfg.Internal.Hooks = hooksWrapper
	corecfg.Internal.CleanUpHooks = cleanupHooksWrapper
	corecfg.Internal.ClientRecordHooks = clientRecordHooksWrapper
	corecfg.Internal.DisableCompoundOps = config.Internal.DisableCompoundOps
	corecfg.Internal.DisableCBD3838Fix = config.Internal.DisableCBD3838Fix
	corecfg.Internal.SerialUnstaging = config.Internal.SerialUnstaging
	corecfg.Internal.NumATRs = config.Internal.NumATRs

	txns, err := coretxns.Init(corecfg)
	if err != nil {
		return nil, err
	}

	t.txns = txns
	return t, nil
}

// Config returns the config that was used during the initialization
// of this Transactions object.
func (t *Transactions) Config() Config {
	return t.config
}

// Run runs a lambda to perform a number of operations as part of a
// singular transaction.
func (t *Transactions) Run(logicFn AttemptFunc, perConfig *PerTransactionConfig) (*Result, error) {
	if perConfig == nil {
		perConfig = &PerTransactionConfig{
			DurabilityLevel: t.config.DurabilityLevel,
		}
	}

	txn, err := t.txns.BeginTransaction(&coretxns.PerTransactionConfig{
		DurabilityLevel: coretxns.DurabilityLevel(perConfig.DurabilityLevel),
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
	var attempts []Attempt
	for {
		err = txn.NewAttempt()
		if err != nil {
			return nil, err
		}

		attempt := AttemptContext{
			txn:        txn,
			transcoder: t.transcoder,
		}

		if t.hooksWrapper != nil {
			t.hooksWrapper.SetAttemptContext(attempt)
		}

		lambdaErr := logicFn(&attempt)

		// The Rollback method when invoked by the user will potentially return nil with
		// no errors indicating that a retry should not be performed.  In gocbcoretxn, this
		// responsibility lies with the caller, so we need to wrap it in gocbtxn.
		if lambdaErr == nil && attempt.rolledBack {
			lambdaErr = errors.New("user initiated rollback")
		}

		var finalErr error
		if lambdaErr == nil {
			if txn.CanCommit() {
				finalErr = attempt.Commit()
			} else if txn.ShouldRollback() {
				finalErr = attempt.Rollback()
			}
		} else {
			finalErr = lambdaErr

			if txn.ShouldRollback() {
				rollbackErr := attempt.Rollback()
				if rollbackErr != nil {
					log.Printf("rollback after error failed: %s", rollbackErr)
				}
			}
		}

		a := txn.Attempt()
		attempts = append(attempts, newAttempt(a))

		if lambdaErr == nil {
			a.PreExpiryAutoRollback = false
		}

		var finalErrCause error
		var txnErr *TransactionOperationFailedError
		var wasUserError bool
		if finalErr != nil {
			if errors.As(finalErr, &txnErr) {
				finalErrCause = txnErr.Unwrap()
				wasUserError = false
			} else {
				wasUserError = true
			}
		} else {
			wasUserError = false
		}

		if !a.Expired && txn.ShouldRetry() && !wasUserError {
			time.Sleep(backoffCalc())
			continue
		}

		switch a.State {
		case coretxns.AttemptStateNothingWritten:
			fallthrough
		case coretxns.AttemptStatePending:
			fallthrough
		case coretxns.AttemptStateAborted:
			fallthrough
		case coretxns.AttemptStateRolledBack:
			if a.Expired && !a.PreExpiryAutoRollback && !wasUserError {
				return nil, &TransactionExpiredError{
					result: &Result{
						TransactionID:     txn.ID(),
						Attempts:          attempts,
						MutationState:     gocb.MutationState{},
						UnstagingComplete: false,
					},
				}
			}

			return nil, &TransactionFailedError{
				cause: finalErrCause,
				result: &Result{
					TransactionID:     txn.ID(),
					Attempts:          attempts,
					MutationState:     gocb.MutationState{},
					UnstagingComplete: false,
				},
			}
		case coretxns.AttemptStateCommitting:
			return nil, &TransactionCommitAmbiguousError{
				cause: finalErrCause,
				result: &Result{
					TransactionID:     txn.ID(),
					Attempts:          attempts,
					MutationState:     gocb.MutationState{},
					UnstagingComplete: false,
				},
			}
		case coretxns.AttemptStateCommitted:
			fallthrough
		case coretxns.AttemptStateCompleted:
			unstagingComplete := a.State == coretxns.AttemptStateCompleted

			mtState := gocb.MutationState{}
			if unstagingComplete {
				for _, tok := range a.MutationState {
					mtState.Internal().Add(tok.BucketName, tok.MutationToken)
				}
			}

			return &Result{
				TransactionID:     txn.ID(),
				Attempts:          attempts,
				MutationState:     mtState,
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

// Close will shut down this Transactions object, shutting down all
// background tasks associated with it.
func (t *Transactions) Close() error {
	return t.txns.Close()
}

func (t *Transactions) agentProvider(bucketName string) (*gocbcore.Agent, error) {
	b := t.cluster.Bucket(bucketName)
	return b.Internal().IORouter()
}

func (t *Transactions) bucketListProvider() ([]string, error) {
	b, err := t.cluster.Buckets().GetAllBuckets(&gocb.GetAllBucketsOptions{
		Timeout: t.config.KeyValueTimeout,
	})
	if err != nil {
		return nil, err
	}

	var names []string
	for name := range b {
		names = append(names, name)
	}
	return names, nil
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
