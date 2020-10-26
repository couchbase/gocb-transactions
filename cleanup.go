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

	return &coreCleanerWrapper{
		wrapped: coretxns.NewCleaner(&coretxns.Config{
			DurabilityLevel: coretxns.DurabilityLevel(config.DurabilityLevel),
			KeyValueTimeout: config.KeyValueTimeout,
			Internal: struct {
				Hooks           coretxns.TransactionHooks
				CleanUpHooks    coretxns.CleanUpHooks
				SerialUnstaging bool
			}{
				Hooks:           nil,
				CleanUpHooks:    cleanupHooksWrapper,
				SerialUnstaging: config.Internal.SerialUnstaging,
			},
			CleanupQueueSize:    config.CleanupQueueSize,
			BucketAgentProvider: agentProvider,
		}),
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
	}
}

func cleanupRequestToCore(request *CleanupRequest) *coretxns.CleanupRequest {
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
