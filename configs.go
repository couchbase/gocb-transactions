package transactions

import "time"

type DurabilityLevel int

const (
	DurabilityLevelNone                       = DurabilityLevel(1)
	DurabilityLevelMajority                   = DurabilityLevel(2)
	DurabilityLevelMajorityAndPersistToActive = DurabilityLevel(3)
	DurabilityLevelPersistToMajority          = DurabilityLevel(4)
)

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
}

type PerTransactionConfig struct {
	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel DurabilityLevel
}
