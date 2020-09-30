package transactions

import (
	"errors"
	"math"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type AttemptFunc func(*AttemptContext) error

type Transactions struct {
	config     Config
	cluster    *gocb.Cluster
	transcoder gocb.Transcoder

	txns         *coretxns.Transactions
	hooksWrapper hooksWrapper
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
			Hooks: config.Internal.Hooks.TransactionHooks(),
		}
	}

	txns, err := coretxns.Init(&coretxns.Config{
		DurabilityLevel: coretxns.DurabilityLevel(config.DurabilityLevel),
		KeyValueTimeout: config.KeyValueTimeout,
		Internal:        struct{ Hooks coretxns.TransactionHooks }{Hooks: hooksWrapper},
	})
	if err != nil {
		return nil, err
	}

	return &Transactions{
		cluster:      cluster,
		config:       *config,
		txns:         txns,
		transcoder:   gocb.NewJSONTranscoder(),
		hooksWrapper: hooksWrapper,
	}, nil
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

		lambdaErr := t.runLambda(logicFn, attempt)
		if lambdaErr != nil {
			var txnErr *TransactionOperationFailedError
			if !errors.As(lambdaErr, &txnErr) {
				txnErr = &TransactionOperationFailedError{
					errorCause: lambdaErr,
					errorClass: coretxns.ErrorClassFailOther,
				}
			}

			a := txn.Attempt()
			if !txnErr.Rollback() || attempt.rolledBack {
				attempts = append(attempts, newAttempt(a))

				if a.Internal.Expired {
					return nil, createTransactionError(attempts, a, txn.ID(), &TransactionOperationFailedError{
						errorCause:  coretxns.ErrAttemptExpired,
						errorClass:  coretxns.ErrorClassFailExpiry,
						shouldRaise: coretxns.ErrorReasonTransactionExpired,
					})
				}

				if txnErr.Retry() {
					time.Sleep(backoffCalc())
					continue
				}

				return nil, createTransactionError(attempts, a, txn.ID(), txnErr)
			}

			err = attempt.Rollback()
			a = txn.Attempt()
			attempts = append(attempts, newAttempt(a))
			if err != nil {
				var txnErr *TransactionOperationFailedError
				if !errors.As(lambdaErr, &txnErr) {
					txnErr = &TransactionOperationFailedError{
						errorCause: lambdaErr,
						errorClass: coretxns.ErrorClassFailOther,
					}
				}

				return nil, createTransactionError(attempts, a, txn.ID(), txnErr)
			}

			if a.Internal.Expired {
				return nil, createTransactionError(attempts, a, txn.ID(), &TransactionOperationFailedError{
					errorCause:  coretxns.ErrAttemptExpired,
					errorClass:  coretxns.ErrorClassFailExpiry,
					shouldRaise: coretxns.ErrorReasonTransactionExpired,
				})
			}

			if txnErr.Retry() {
				time.Sleep(backoffCalc())
				continue
			}

			return nil, createTransactionError(attempts, a, txn.ID(), txnErr)
		}

		a := txn.Attempt()
		attempts = append(attempts, newAttempt(a))

		return createResult(attempts, a, txn.ID()), nil
	}
}

func (t *Transactions) runLambda(logicFn AttemptFunc, attempt AttemptContext) error {
	err := logicFn(&attempt)
	if err != nil {
		return err
	}

	if !attempt.committed && !attempt.rolledBack {
		err := attempt.Commit()
		if err != nil {
			return err
		}
	}

	return nil

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
	return errors.New("not implemented")
}
