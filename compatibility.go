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
	coretxns "github.com/couchbase/gocbcore-transactions"
)

// ProtocolVersion returns the protocol version that this library supports.
func ProtocolVersion() string {
	return coretxns.ProtocolVersion()
}

// ProtocolExtensions returns a list strings representing the various features
// that this specific version of the library supports within its protocol version.
func ProtocolExtensions() []string {
	return coretxns.ProtocolExtensions()
}
