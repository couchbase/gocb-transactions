package transactions

import coretxns "github.com/couchbaselabs/gocbcore-transactions"

// TransactionHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type TransactionHooks interface {
	BeforeATRCommit(ctx AttemptContext) error
	AfterATRCommit(ctx AttemptContext) error
	BeforeDocCommitted(ctx AttemptContext, docID string) error
	BeforeRemovingDocDuringStagedInsert(ctx AttemptContext, docID string) error
	BeforeRollbackDeleteInserted(ctx AttemptContext, docID string) error
	AfterDocCommittedBeforeSavingCAS(ctx AttemptContext, docID string) error
	AfterDocCommitted(ctx AttemptContext, docID string) error
	BeforeStagedInsert(ctx AttemptContext, docID string) error
	BeforeStagedRemove(ctx AttemptContext, docID string) error
	BeforeStagedReplace(ctx AttemptContext, docID string) error
	BeforeDocRemoved(ctx AttemptContext, docID string) error
	BeforeDocRolledBack(ctx AttemptContext, docID string) error
	AfterDocRemovedPreRetry(ctx AttemptContext, docID string) error
	AfterDocRemovedPostRetry(ctx AttemptContext, docID string) error
	AfterGetComplete(ctx AttemptContext, docID string) error
	AfterStagedReplaceComplete(ctx AttemptContext, docID string) error
	AfterStagedRemoveComplete(ctx AttemptContext, docID string) error
	AfterStagedInsertComplete(ctx AttemptContext, docID string) error
	AfterRollbackReplaceOrRemove(ctx AttemptContext, docID string) error
	AfterRollbackDeleteInserted(ctx AttemptContext, docID string) error
	BeforeCheckATREntryForBlockingDoc(ctx AttemptContext, docID string) error
	BeforeDocGet(ctx AttemptContext, docID string) error
	BeforeGetDocInExistsDuringStagedInsert(ctx AttemptContext, docID string) error
	AfterDocsCommitted(ctx AttemptContext) error
	AfterDocsRemoved(ctx AttemptContext) error
	AfterATRPending(ctx AttemptContext) error
	BeforeATRPending(ctx AttemptContext) error
	BeforeATRComplete(ctx AttemptContext) error
	BeforeATRRolledBack(ctx AttemptContext) error
	AfterATRComplete(ctx AttemptContext) error
	BeforeATRAborted(ctx AttemptContext) error
	AfterATRAborted(ctx AttemptContext) error
	AfterATRRolledBack(ctx AttemptContext) error
	RandomATRIDForVbucket(ctx AttemptContext, vbID string) (string, error)
	HasExpiredClientSideHook(ctx AttemptContext, stage string, vbID string) (bool, error)
}

// CleanUpHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type CleanUpHooks interface {
	BeforeATRGet(id string) error
	BeforeDocGet(id string) error
	BeforeRemoveLinks(id string) error
	BeforeCommitDoc(id string) error
	BeforeRemoveDocStagedForRemoval(id string) error
	BeforeRemoveDoc(id string) error
	BeforeATRRemove() error
}

// ClientRecordHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type ClientRecordHooks interface {
	BeforeCreateRecord() error
	BeforeRemoveClient() error
	BeforeUpdateCAS() error
	BeforeGetRecord() error
	BeforeUpdateRecord() error
}

// Hooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type Hooks interface {
	TransactionHooks() TransactionHooks
	CleanUpHooks() CleanUpHooks
	ClientRecordHooks() ClientRecordHooks
}

type hooksWrapper interface {
	SetAttemptContext(ctx AttemptContext)
	coretxns.TransactionHooks
}

type coreTxnsHooksWrapper struct {
	ctx   AttemptContext
	Hooks TransactionHooks
}

func (cthw *coreTxnsHooksWrapper) SetAttemptContext(ctx AttemptContext) {
	cthw.ctx = ctx
}

func (cthw *coreTxnsHooksWrapper) BeforeATRCommit() error {
	return cthw.Hooks.BeforeATRCommit(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) AfterATRCommit() error {
	return cthw.Hooks.AfterATRCommit(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) BeforeDocCommitted(docID []byte) error {
	return cthw.Hooks.BeforeDocCommitted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeRemovingDocDuringStagedInsert(docID []byte) error {
	return cthw.Hooks.BeforeRemovingDocDuringStagedInsert(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeRollbackDeleteInserted(docID []byte) error {
	return cthw.Hooks.BeforeRollbackDeleteInserted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterDocCommittedBeforeSavingCAS(docID []byte) error {
	return cthw.Hooks.AfterDocCommittedBeforeSavingCAS(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterDocCommitted(docID []byte) error {
	return cthw.Hooks.AfterDocCommitted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedInsert(docID []byte) error {
	return cthw.Hooks.BeforeStagedInsert(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedRemove(docID []byte) error {
	return cthw.Hooks.BeforeStagedRemove(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedReplace(docID []byte) error {
	return cthw.Hooks.BeforeStagedReplace(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeDocRemoved(docID []byte) error {
	return cthw.Hooks.BeforeDocRemoved(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeDocRolledBack(docID []byte) error {
	return cthw.Hooks.BeforeDocRolledBack(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterDocRemovedPreRetry(docID []byte) error {
	return cthw.Hooks.AfterDocRemovedPreRetry(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterDocRemovedPostRetry(docID []byte) error {
	return cthw.Hooks.AfterDocRemovedPostRetry(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterGetComplete(docID []byte) error {
	return cthw.Hooks.AfterGetComplete(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterStagedReplaceComplete(docID []byte) error {
	return cthw.Hooks.AfterStagedReplaceComplete(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterStagedRemoveComplete(docID []byte) error {
	return cthw.Hooks.BeforeDocCommitted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterStagedInsertComplete(docID []byte) error {
	return cthw.Hooks.BeforeDocCommitted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterRollbackReplaceOrRemove(docID []byte) error {
	return cthw.Hooks.BeforeDocCommitted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterRollbackDeleteInserted(docID []byte) error {
	return cthw.Hooks.BeforeDocCommitted(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeCheckATREntryForBlockingDoc(docID []byte) error {
	return cthw.Hooks.BeforeCheckATREntryForBlockingDoc(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeDocGet(docID []byte) error {
	return cthw.Hooks.BeforeDocGet(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) BeforeGetDocInExistsDuringStagedInsert(docID []byte) error {
	return cthw.Hooks.BeforeGetDocInExistsDuringStagedInsert(cthw.ctx, string(docID))
}

func (cthw *coreTxnsHooksWrapper) AfterDocsCommitted() error {
	return cthw.Hooks.AfterDocsCommitted(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) AfterDocsRemoved() error {
	return cthw.Hooks.AfterDocsRemoved(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) AfterATRPending() error {
	return cthw.Hooks.AfterATRPending(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) BeforeATRPending() error {
	return cthw.Hooks.BeforeATRPending(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) BeforeATRComplete() error {
	return cthw.Hooks.BeforeATRComplete(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) BeforeATRRolledBack() error {
	return cthw.Hooks.BeforeATRRolledBack(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) AfterATRComplete() error {
	return cthw.Hooks.AfterATRComplete(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) BeforeATRAborted() error {
	return cthw.Hooks.BeforeATRAborted(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) AfterATRAborted() error {
	return cthw.Hooks.AfterATRAborted(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) AfterATRRolledBack() error {
	return cthw.Hooks.AfterATRRolledBack(cthw.ctx)
}

func (cthw *coreTxnsHooksWrapper) RandomATRIDForVbucket(vbID []byte) (string, error) {
	return cthw.Hooks.RandomATRIDForVbucket(cthw.ctx, string(vbID))
}

func (cthw *coreTxnsHooksWrapper) HasExpiredClientSideHook(stage string, vbID []byte) (bool, error) {
	return cthw.Hooks.HasExpiredClientSideHook(cthw.ctx, stage, string(vbID))
}

type noopHooksWrapper struct {
}

func (nhw *noopHooksWrapper) SetAttemptContext(ctx AttemptContext) {
}

func (nhw *noopHooksWrapper) BeforeATRCommit() error {
	return nil
}

func (nhw *noopHooksWrapper) AfterATRCommit() error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeDocCommitted(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeRemovingDocDuringStagedInsert(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeRollbackDeleteInserted(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterDocCommittedBeforeSavingCAS(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterDocCommitted(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeStagedInsert(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeStagedRemove(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeStagedReplace(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeDocRemoved(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeDocRolledBack(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterDocRemovedPreRetry(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterDocRemovedPostRetry(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterGetComplete(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterStagedReplaceComplete(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterStagedRemoveComplete(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterStagedInsertComplete(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterRollbackReplaceOrRemove(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterRollbackDeleteInserted(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeCheckATREntryForBlockingDoc(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeDocGet(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeGetDocInExistsDuringStagedInsert(docID []byte) error {
	return nil
}

func (nhw *noopHooksWrapper) AfterDocsCommitted() error {
	return nil
}

func (nhw *noopHooksWrapper) AfterDocsRemoved() error {
	return nil
}

func (nhw *noopHooksWrapper) AfterATRPending() error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeATRPending() error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeATRComplete() error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeATRRolledBack() error {
	return nil
}

func (nhw *noopHooksWrapper) AfterATRComplete() error {
	return nil
}

func (nhw *noopHooksWrapper) BeforeATRAborted() error {
	return nil
}

func (nhw *noopHooksWrapper) AfterATRAborted() error {
	return nil
}

func (nhw *noopHooksWrapper) AfterATRRolledBack() error {
	return nil
}

func (nhw *noopHooksWrapper) RandomATRIDForVbucket(vbID []byte) (string, error) {
	return "", nil
}

func (nhw *noopHooksWrapper) HasExpiredClientSideHook(stage string, vbID []byte) (bool, error) {
	return false, nil
}
