package transactions

import (
	"errors"
	"log"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

type AttemptFunc func(*AttemptContext) error

type Transactions struct {
	config Config
}

// Init will initialize the transactions library and return a Transactions
// object which can be used to perform transactions.
func Init(cluster *gocb.Cluster, config *Config) (*Transactions, error) {
	log.Printf("skipping unimplemented Init")
	return nil, nil
}

// Config returns the config that was used during the initialization
// of this Transactions object.
func (t *Transactions) Config() Config {
	return t.config
}

// Run runs a lambda to perform a number of operations as part of a
// singular transaction.
func (t *Transactions) Run(logicFn AttemptFunc, perConfig *PerTransactionConfig) error {
	transactionUUID := uuid.New().String()
	log.Printf("Starting Transaction %s", transactionUUID)

	expiryTime := time.Now().Add(15 * time.Second)

	attemptUUID := uuid.New().String()
	attempt := AttemptContext{
		transactionID:       transactionUUID,
		id:                  attemptUUID,
		state:               attemptStateNothingWritten,
		stagedMutations:     nil,
		finalMutationTokens: gocb.MutationState{},
		atrID:               -1,
		atrCollection:       nil,
		expiryOvertimeMode:  false,
		expiryTime:          expiryTime,
	}

	err := logicFn(&attempt)

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
