package transactions

import (
	"errors"

	gocb "github.com/couchbase/gocb/v2"
)

type AttemptFunc func(*AttemptContext) error

type Transactions struct {
	config Config
}

func Init(cluster *gocb.Cluster, config *Config) (*Transactions, error) {
	return nil, errors.New("not implemented")
}

func (t *Transactions) Config() Config {
	return t.config
}

func (t *Transactions) Run(logicFn AttemptFunc, perConfig *PerTransactionConfig) error {
	return errors.New("not implemented")
}

func (t *Transactions) Commit(serialized SerializedContext, perConfig *PerTransactionConfig) error {
	return errors.New("not implemented")
}

func (t *Transactions) Rollback(serialized SerializedContext, perConfig *PerTransactionConfig) error {
	return errors.New("not implemented")
}

func (t *Transactions) Close() error {
	return errors.New("not implemented")
}
