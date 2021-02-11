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

// SerializedContext represents a transaction which has been serialized
// for resumption at a later point in time.
type SerializedContext struct {
}

// EncodeAsString will encode this SerializedContext to a string which
// can be decoded later to resume the transaction.
func (c *SerializedContext) EncodeAsString() string {
	return ""
}

// EncodeAsBytes will encode this SerializedContext to a set of bytes which
// can be decoded later to resume the transaction.
func (c *SerializedContext) EncodeAsBytes() []byte {
	return []byte{}
}
