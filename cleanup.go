package transactions

import (
	"github.com/couchbase/gocb/v2"
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
)

// DocRecord represents an individual document operation requiring cleanup.
// Internal: This should never be used and is not supported.
type DocRecord struct {
	CollectionName string
	ScopeName      string
	BucketName     string
	ID             string
}

// CleanupAttempt represents the result of running cleanup for a transaction attempt.
// Internal: This should never be used and is not supported.
type CleanupAttempt struct {
	Success           bool
	IsReqular         bool
	AttemptID         string
	AtrID             string
	AtrCollectionName string
	AtrScopeName      string
	AtrBucketName     string
	Request           *CleanupRequest
}

// CleanupRequest represents a complete transaction attempt that requires cleanup.
// Internal: This should never be used and is not supported.
type CleanupRequest struct {
	AttemptID         string
	AtrID             string
	AtrCollectionName string
	AtrScopeName      string
	AtrBucketName     string
	Inserts           []DocRecord
	Replaces          []DocRecord
	Removes           []DocRecord
	State             AttemptState
	ForwardCompat     map[string][]ForwardCompatibilityEntry
}

// ForwardCompatibilityEntry represents a forward compatibility entry.
// Internal: This should never be used and is not supported.
type ForwardCompatibilityEntry struct {
	ProtocolVersion   string `json:"p,omitempty"`
	ProtocolExtension string `json:"e,omitempty"`
	Behaviour         string `json:"b,omitempty"`
	RetryInterval     int    `json:"ra,omitempty"`
}

// ClientRecordDetails is the result of processing a client record.
// Internal: This should never be used and is not supported.
type ClientRecordDetails struct {
	NumActiveClients     int
	IndexOfThisClient    int
	ClientIsNew          bool
	ExpiredClientIDs     []string
	NumExistingClients   int
	NumExpiredClients    int
	OverrideEnabled      bool
	OverrideActive       bool
	OverrideExpiresCas   int64
	CasNowNanos          int64
	AtrsHandledByClient  []string
	CheckAtrEveryNMillis int
	ClientUUID           string
}

// ProcessATRStats is the stats recorded when running a ProcessATR request.
// Internal: This should never be used and is not supported.
type ProcessATRStats struct {
	NumEntries        int
	NumEntriesExpired int
}

// Cleaner is responsible for performing cleanup of completed transactions.
// Internal: This should never be used and is not supported.
type Cleaner interface {
	AddRequest(req *CleanupRequest) bool
	PopRequest() *CleanupRequest
	ForceCleanupQueue() []CleanupAttempt
	QueueLength() int32
	CleanupAttempt(bucket *gocb.Bucket, isRegular bool, req *CleanupRequest) CleanupAttempt
	Close()
}

// NewCleaner returns a Cleaner implementation.
// Internal: This should never be used and is not supported.
func NewCleaner(agentProvider coretxns.BucketAgentProviderFn, config *Config) Cleaner {
	cleanupHooksWrapper := &coreTxnsCleanupHooksWrapper{
		CleanupHooks: config.Internal.CleanupHooks,
	}

	corecfg := &coretxns.Config{}
	corecfg.DurabilityLevel = coretxns.DurabilityLevel(config.DurabilityLevel)
	corecfg.KeyValueTimeout = config.KeyValueTimeout
	corecfg.Internal.Hooks = nil
	corecfg.CleanupQueueSize = config.CleanupQueueSize
	corecfg.BucketAgentProvider = agentProvider
	corecfg.Internal.CleanUpHooks = cleanupHooksWrapper
	corecfg.Internal.DisableCompoundOps = config.Internal.DisableCompoundOps
	corecfg.Internal.DisableCBD3838Fix = config.Internal.DisableCBD3838Fix
	corecfg.Internal.SerialUnstaging = config.Internal.SerialUnstaging
	corecfg.Internal.NumATRs = config.Internal.NumATRs

	return &coreCleanerWrapper{
		wrapped: coretxns.NewCleaner(corecfg),
	}
}

type coreCleanerWrapper struct {
	wrapped coretxns.Cleaner
}

func (ccw *coreCleanerWrapper) AddRequest(req *CleanupRequest) bool {
	return ccw.wrapped.AddRequest(cleanupRequestToCore(req))
}

func (ccw *coreCleanerWrapper) PopRequest() *CleanupRequest {
	return cleanupRequestFromCore(ccw.wrapped.PopRequest())
}

func (ccw *coreCleanerWrapper) ForceCleanupQueue() []CleanupAttempt {
	waitCh := make(chan []CleanupAttempt, 1)
	ccw.wrapped.ForceCleanupQueue(func(coreAttempts []coretxns.CleanupAttempt) {
		var attempts []CleanupAttempt
		for _, attempt := range coreAttempts {
			attempts = append(attempts, cleanupAttemptFromCore(attempt))
		}
		waitCh <- attempts
	})
	return <-waitCh
}

func (ccw *coreCleanerWrapper) QueueLength() int32 {
	return ccw.wrapped.QueueLength()
}

func (ccw *coreCleanerWrapper) CleanupAttempt(bucket *gocb.Bucket, isRegular bool, req *CleanupRequest) CleanupAttempt {
	waitCh := make(chan CleanupAttempt, 1)
	a, err := bucket.Internal().IORouter()
	if err != nil {
		return CleanupAttempt{
			Success:           false,
			IsReqular:         isRegular,
			AttemptID:         req.AttemptID,
			AtrID:             req.AtrID,
			AtrCollectionName: req.AtrCollectionName,
			AtrScopeName:      req.AtrScopeName,
			AtrBucketName:     req.AtrBucketName,
			Request:           req,
		}
	}
	ccw.wrapped.CleanupAttempt(a, cleanupRequestToCore(req), isRegular, func(attempt coretxns.CleanupAttempt) {
		waitCh <- cleanupAttemptFromCore(attempt)
	})
	return <-waitCh
}

func (ccw *coreCleanerWrapper) Close() {
	ccw.wrapped.Close()
}

// LostTransactionCleaner is responsible for performing cleanup of lost transactions.
// Internal: This should never be used and is not supported.
type LostTransactionCleaner interface {
	ProcessATR(bucket *gocb.Bucket, collection, scope, atrID string) ([]CleanupAttempt, ProcessATRStats)
	ProcessClient(bucket *gocb.Bucket, collection, scope, clientUUID string) (*ClientRecordDetails, error)
	RemoveClient(uuid string) error
	Close()
}

type coreLostCleanerWrapper struct {
	wrapped coretxns.LostTransactionCleaner
}

func (clcw *coreLostCleanerWrapper) ProcessATR(bucket *gocb.Bucket, collection, scope, atrID string) ([]CleanupAttempt, ProcessATRStats) {
	a, err := bucket.Internal().IORouter()
	if err != nil {
		return nil, ProcessATRStats{}
	}

	var ourAttempts []CleanupAttempt
	var ourStats ProcessATRStats
	waitCh := make(chan struct{}, 1)
	clcw.wrapped.ProcessATR(a, collection, scope, atrID, func(attempts []coretxns.CleanupAttempt, stats coretxns.ProcessATRStats) {
		for _, a := range attempts {
			ourAttempts = append(ourAttempts, cleanupAttemptFromCore(a))
		}
		ourStats = ProcessATRStats(stats)

		waitCh <- struct{}{}
	})

	<-waitCh
	return ourAttempts, ourStats
}

func (clcw *coreLostCleanerWrapper) ProcessClient(bucket *gocb.Bucket, collection, scope, clientUUID string) (*ClientRecordDetails, error) {
	type result struct {
		recordDetails *ClientRecordDetails
		err           error
	}
	waitCh := make(chan result, 1)
	a, err := bucket.Internal().IORouter()
	if err != nil {
		return nil, err
	}

	clcw.wrapped.ProcessClient(a, collection, scope, clientUUID, func(details *coretxns.ClientRecordDetails, err error) {
		if err != nil {
			waitCh <- result{
				err: err,
			}
			return
		}
		waitCh <- result{
			recordDetails: &ClientRecordDetails{
				NumActiveClients:     details.NumActiveClients,
				IndexOfThisClient:    details.IndexOfThisClient,
				ClientIsNew:          details.ClientIsNew,
				ExpiredClientIDs:     details.ExpiredClientIDs,
				NumExistingClients:   details.NumExistingClients,
				NumExpiredClients:    details.NumExpiredClients,
				OverrideEnabled:      details.OverrideEnabled,
				OverrideActive:       details.OverrideActive,
				OverrideExpiresCas:   details.OverrideExpiresCas,
				CasNowNanos:          details.CasNowNanos,
				AtrsHandledByClient:  details.AtrsHandledByClient,
				CheckAtrEveryNMillis: details.CheckAtrEveryNMillis,
				ClientUUID:           details.ClientUUID,
			},
		}
	})

	res := <-waitCh
	return res.recordDetails, res.err
}

func (clcw *coreLostCleanerWrapper) RemoveClient(uuid string) error {
	return clcw.wrapped.RemoveClientFromAllBuckets(uuid)
}

func (clcw *coreLostCleanerWrapper) Close() {
	clcw.wrapped.Close()
}

// NewLostCleanup returns a LostCleanup implementation.
// Internal: This should never be used and is not supported.
func NewLostCleanup(agentProvider coretxns.BucketAgentProviderFn, locationProvider coretxns.LostCleanupATRLocationProviderFn,
	config *Config) LostTransactionCleaner {
	cleanupHooksWrapper := &coreTxnsClientRecordHooksWrapper{
		coreTxnsCleanupHooksWrapper: coreTxnsCleanupHooksWrapper{
			CleanupHooks: config.Internal.CleanupHooks,
		},
		ClientRecordHooks: config.Internal.ClientRecordHooks,
	}

	corecfg := &coretxns.Config{}
	corecfg.DurabilityLevel = coretxns.DurabilityLevel(config.DurabilityLevel)
	corecfg.KeyValueTimeout = config.KeyValueTimeout
	corecfg.Internal.Hooks = nil
	corecfg.CleanupQueueSize = config.CleanupQueueSize
	corecfg.BucketAgentProvider = agentProvider
	corecfg.LostCleanupATRLocationProvider = locationProvider
	corecfg.Internal.CleanUpHooks = cleanupHooksWrapper
	corecfg.Internal.ClientRecordHooks = cleanupHooksWrapper
	corecfg.Internal.DisableCompoundOps = config.Internal.DisableCompoundOps
	corecfg.Internal.DisableCBD3838Fix = config.Internal.DisableCBD3838Fix
	corecfg.Internal.SerialUnstaging = config.Internal.SerialUnstaging
	corecfg.Internal.NumATRs = config.Internal.NumATRs

	return &coreLostCleanerWrapper{
		wrapped: coretxns.NewLostTransactionCleaner(corecfg),
	}
}

func cleanupAttemptFromCore(attempt coretxns.CleanupAttempt) CleanupAttempt {
	var req *CleanupRequest
	if attempt.Request != nil {
		req = &CleanupRequest{
			AttemptID:         attempt.Request.AttemptID,
			AtrID:             string(attempt.Request.AtrID),
			AtrCollectionName: attempt.Request.AtrCollectionName,
			AtrScopeName:      attempt.Request.AtrScopeName,
			AtrBucketName:     attempt.Request.AtrBucketName,
			Inserts:           docRecordsFromCore(attempt.Request.Inserts),
			Replaces:          docRecordsFromCore(attempt.Request.Replaces),
			Removes:           docRecordsFromCore(attempt.Request.Removes),
			State:             AttemptState(attempt.Request.State),
		}
	}
	return CleanupAttempt{
		Success:           attempt.Success,
		IsReqular:         attempt.IsReqular,
		AttemptID:         attempt.AttemptID,
		AtrID:             string(attempt.AtrID),
		AtrCollectionName: attempt.AtrCollectionName,
		AtrScopeName:      attempt.AtrScopeName,
		AtrBucketName:     attempt.AtrBucketName,
		Request:           req,
	}
}

func docRecordsFromCore(drs []coretxns.DocRecord) []DocRecord {
	var recs []DocRecord
	for _, i := range drs {
		recs = append(recs, DocRecord{
			CollectionName: i.CollectionName,
			ScopeName:      i.ScopeName,
			BucketName:     i.BucketName,
			ID:             string(i.ID),
		})
	}

	return recs
}

func cleanupRequestFromCore(request *coretxns.CleanupRequest) *CleanupRequest {
	forwardCompat := make(map[string][]ForwardCompatibilityEntry)
	for k, entries := range request.ForwardCompat {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]ForwardCompatibilityEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = ForwardCompatibilityEntry(entry)
		}
	}

	return &CleanupRequest{
		AttemptID:         request.AttemptID,
		AtrID:             string(request.AtrID),
		AtrCollectionName: request.AtrCollectionName,
		AtrScopeName:      request.AtrScopeName,
		AtrBucketName:     request.AtrBucketName,
		Inserts:           docRecordsFromCore(request.Inserts),
		Replaces:          docRecordsFromCore(request.Replaces),
		Removes:           docRecordsFromCore(request.Removes),
		State:             AttemptState(request.State),
		ForwardCompat:     forwardCompat,
	}
}

func cleanupRequestToCore(request *CleanupRequest) *coretxns.CleanupRequest {
	forwardCompat := make(map[string][]coretxns.ForwardCompatibilityEntry)
	for k, entries := range request.ForwardCompat {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]coretxns.ForwardCompatibilityEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = coretxns.ForwardCompatibilityEntry(entry)
		}
	}

	return &coretxns.CleanupRequest{
		AttemptID:         request.AttemptID,
		AtrID:             []byte(request.AtrID),
		AtrCollectionName: request.AtrCollectionName,
		AtrScopeName:      request.AtrScopeName,
		AtrBucketName:     request.AtrBucketName,
		Inserts:           docRecordsToCore(request.Inserts),
		Replaces:          docRecordsToCore(request.Replaces),
		Removes:           docRecordsToCore(request.Removes),
		State:             coretxns.AttemptState(request.State),
		ForwardCompat:     forwardCompat,
	}
}

func docRecordsToCore(drs []DocRecord) []coretxns.DocRecord {
	var recs []coretxns.DocRecord
	for _, i := range drs {
		recs = append(recs, coretxns.DocRecord{
			CollectionName: i.CollectionName,
			ScopeName:      i.ScopeName,
			BucketName:     i.BucketName,
			ID:             []byte(i.ID),
		})
	}

	return recs
}
