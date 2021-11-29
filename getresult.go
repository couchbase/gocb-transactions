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
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbase/gocbcore-transactions"
	"github.com/couchbase/gocbcore/v10"
	"strconv"
)

// GetResult represents the result of a Get operation which was performed.
type GetResult struct {
	collection *gocb.Collection
	docID      string

	transcoder gocb.Transcoder
	flags      uint32

	txnMeta json.RawMessage

	coreRes *coretxns.GetResult
}

// Content provides access to the documents contents.
func (d *GetResult) Content(valuePtr interface{}) error {
	return d.transcoder.Decode(d.coreRes.Value, d.flags, valuePtr)
}

func fromScas(scas string) (gocbcore.Cas, error) {
	i, err := strconv.ParseUint(scas, 10, 64)
	if err != nil {
		return 0, err
	}

	return gocbcore.Cas(i), nil
}

func toScas(cas gocbcore.Cas) string {
	return strconv.FormatUint(uint64(cas), 10)
}
