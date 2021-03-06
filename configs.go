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
	"time"

	"github.com/couchbase/gocb/v2"

	coretxns "github.com/couchbase/gocbcore-transactions"
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
	// MetadataCollection specifies a specific location to place meta-data.
	MetadataCollection *gocb.Collection

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
		Hooks             TransactionHooks
		CleanupHooks      CleanupHooks
		ClientRecordHooks ClientRecordHooks
		NumATRs           int
	}
}

// PerTransactionConfig specifies options which can be overriden on a per transaction basis.
type PerTransactionConfig struct {
	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel DurabilityLevel
}

// ATRLocation specifies a specific location where ATR entries should be
// placed when performing transactions.
type ATRLocation struct {
	BucketName     string
	ScopeName      string
	CollectionName string
}
