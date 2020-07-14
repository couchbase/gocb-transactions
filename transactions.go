package transactions

import (
	"errors"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type AttemptFunc func(*AttemptContext) error

type Transactions struct {
	config  Config
	cluster *gocb.Cluster

	txns *coretxns.Transactions
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

	txns, err := coretxns.Init(&coretxns.Config{
		DurabilityLevel: coretxns.DurabilityLevel(config.DurabilityLevel),
		KeyValueTimeout: config.KeyValueTimeout,
	})
	if err != nil {
		return nil, err
	}

	return &Transactions{
		cluster: cluster,
		config:  *config,
		txns:    txns,
	}, nil
}

// Config returns the config that was used during the initialization
// of this Transactions object.
func (t *Transactions) Config() Config {
	return t.config
}

// Run runs a lambda to perform a number of operations as part of a
// singular transaction.
func (t *Transactions) Run(logicFn AttemptFunc, perConfig *PerTransactionConfig) error {
	if perConfig == nil {
		perConfig = &PerTransactionConfig{
			DurabilityLevel: t.config.DurabilityLevel,
		}
	}

	txn, err := t.txns.BeginTransaction(&coretxns.PerTransactionConfig{
		DurabilityLevel: coretxns.DurabilityLevel(perConfig.DurabilityLevel),
	})
	if err != nil {
		return err
	}

	err = txn.NewAttempt()
	if err != nil {
		return err
	}

	attempt := AttemptContext{
		txn: txn,
	}

	err = logicFn(&attempt)

	return err
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
