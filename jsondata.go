package transactions

type jsonAtrMutation struct {
	ScopeName      string `json:"scp,omitempty"`
	CollectionName string `json:"col,omitempty"`
	BucketName     string `json:"bkt,omitempty"`
	DocID          string `json:"id,omitempty"`
}

type jsonAtrAttempt struct {
	TransactionID string `json:"tid,omitempty"`
	ExpiryTime    uint   `json:"exp,omitempty"`
	State         string `json:"state,omitempty"`

	PendingCAS    string `json:"tst,omitempty"`
	CommitCAS     string `json:"tsc,omitempty"`
	CompletedCAS  string `json:"tsco,omitempty"`
	AbortCAS      string `json:"tsrs,omitempty"`
	RolledBackCAS string `json:"tsrc,omitempty"`

	Inserts  []jsonAtrMutation `json:"ins,omitempty"`
	Replaces []jsonAtrMutation `json:"rep,omitempty"`
	Removes  []jsonAtrMutation `json:"rem,omitempty"`
}

type jsonTxnXattr struct {
	ID struct {
		Transaction string `json:"txn,omitempty"`
		Attempt     string `json:"atmpt,omitempty"`
	} `json:"id,omitempty"`
	ATR struct {
		DocID          string `json:"id,omitempty"`
		BucketName     string `json:"bkt,omitempty"`
		CollectionName string `json:"coll,omitempty"`
	} `json:"atr,omitempty"`
	Operation struct {
		Type   string      `json:"type,omitempty"`
		Staged interface{} `json:"stgd,omitempty"`
	} `json:"op,omitempty"`
	Restore struct {
		OriginalCAS string `json:"cas,omitempty"`
		ExpiryTime  uint   `json:"exptime,omitempty"`
		RevID       string `json:"revid,omitempty"`
	}
}
