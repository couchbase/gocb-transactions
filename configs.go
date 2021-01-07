package transactions

import (
	"time"

	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

// DurabilityLevel specifies the level of synchronous replication to use.
type DurabilityLevel uint8

const (
	// DurabilityLevelUnknown indicates to use the default level.
	DurabilityLevelUnknown = DurabilityLevel(coretxns.DurabilityLevelUnknown)

	// DurabilityLevelNone indicates that no durability is needed.
	DurabilityLevelNone = DurabilityLevel(coretxns.DurabilityLevelNone)

	// DurabilityLevelMajority indicates the operation must be replicated to the majority.
	DurabilityLevelMajority = DurabilityLevel(coretxns.DurabilityLevelMajority)

	// DurabilityLevelMajorityAndPersistToActive indicates the operation must be replicated
	// to the majority and persisted to the active server.
	DurabilityLevelMajorityAndPersistToActive = DurabilityLevel(coretxns.DurabilityLevelMajorityAndPersistToActive)

	// DurabilityLevelPersistToMajority indicates the operation must be persisted to the active server.
	DurabilityLevelPersistToMajority = DurabilityLevel(coretxns.DurabilityLevelPersistToMajority)
)

// Config specifies various tunable options related to transactions.
type Config struct {
	// ExpirationTime sets the maximum time that transactions created
	// by this Transactions object can run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this Transactions object.
	DurabilityLevel DurabilityLevel

	// KeyValueTimeout specifies the default timeout used for all KV writes.
	KeyValueTimeout time.Duration

	// CleanupWindow specifies how often to the cleanup process runs
	// attempting to garbage collection transactions that have failed but
	// were not cleaned up by the previous client.
	CleanupWindow time.Duration

	// CleanupClientAttempts controls where any transaction attempts made
	// by this client are automatically removed.
	CleanupClientAttempts bool

	// CleanupLostAttempts controls where a background process is created
	// to cleanup any ‘lost’ transaction attempts.
	CleanupLostAttempts bool

	// CleanupQueueSize controls the maximum queue size for the cleanup thread.
	CleanupQueueSize uint32

	// Internal specifies a set of options for internal use.
	// Internal: This should never be used and is not supported.
	Internal struct {
		Hooks              TransactionHooks
		CleanupHooks       CleanupHooks
		DisableCompoundOps bool
		SerialUnstaging    bool
		ClientRecordHooks  ClientRecordHooks
		NumATRs            int
	}
}

// PerTransactionConfig specifies options which can be overriden on a per transaction basis.
type PerTransactionConfig struct {
	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel DurabilityLevel
}
