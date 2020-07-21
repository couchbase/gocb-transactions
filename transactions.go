package transactions

import (
	"errors"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type AttemptFunc func(*AttemptContext) error

type Transactions struct {
	config     Config
	cluster    *gocb.Cluster
	transcoder gocb.Transcoder

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
		cluster:    cluster,
		config:     *config,
		txns:       txns,
		transcoder: gocb.NewJSONTranscoder(),
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

		err = logicFn(&attempt)

		if err != nil {
			a := txn.Attempt()
			attempts = append(attempts, Attempt{
				ID:    a.ID,
				State: AttemptState(a.State),
			})
			continue
		}

		if attempt.committed {
			a := txn.Attempt()
			attempts = append(attempts, Attempt{
				ID:    a.ID,
				State: AttemptState(a.State),
			})

			state := &gocb.MutationState{}
			for _, tok := range a.MutationState {
				state.Internal().Add(tok.BucketName, tok.MutationToken)
			}

			return &Result{
				Attempts:          attempts,
				TransactionID:     txn.ID(),
				UnstagingComplete: a.State == coretxns.AttemptStateCompleted,
				MutationState:     *state,
				Internal:          struct{ MutationTokens []gocb.MutationToken }{MutationTokens: state.Internal().Tokens()},
			}, nil
		}

		err = attempt.Commit()
		if err != nil {
			a := txn.Attempt()
			attempts = append(attempts, Attempt{
				ID:    a.ID,
				State: AttemptState(a.State),
			})
			continue
		}

		a := txn.Attempt()
		attempts = append(attempts, Attempt{
			ID:    a.ID,
			State: AttemptState(a.State),
		})

		state := &gocb.MutationState{}
		for _, tok := range a.MutationState {
			state.Internal().Add(tok.BucketName, tok.MutationToken)
		}

		return &Result{
			Attempts:          attempts,
			TransactionID:     txn.ID(),
			UnstagingComplete: a.State == coretxns.AttemptStateCompleted,
			MutationState:     *state,
			Internal:          struct{ MutationTokens []gocb.MutationToken }{MutationTokens: state.Internal().Tokens()},
		}, nil
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
	return errors.New("not implemented")
}
