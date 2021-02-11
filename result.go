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
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

type AttemptState int

const (
	AttemptStateNothingWritten AttemptState = AttemptState(coretxns.AttemptStateNothingWritten)
	AttemptStatePending        AttemptState = AttemptState(coretxns.AttemptStatePending)
	AttemptStateCommitted      AttemptState = AttemptState(coretxns.AttemptStateCommitted)
	AttemptStateCompleted      AttemptState = AttemptState(coretxns.AttemptStateCompleted)
	AttemptStateAborted        AttemptState = AttemptState(coretxns.AttemptStateAborted)
	AttemptStateRolledBack     AttemptState = AttemptState(coretxns.AttemptStateRolledBack)
)

// Result represents the result of a transaction which was executed.
type Result struct {
	// TransactionID represents the UUID assigned to this transaction
	TransactionID string

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool

	// Serialized represents the serialized data from this transaction if
	// the transaction was serialized as opposed to being executed.
	Serialized *SerializedContext
}
