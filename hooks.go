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
	BeforeATRCommitAmbiguityResolution(ctx AttemptContext) error
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

func (cthw *coreTxnsHooksWrapper) BeforeATRCommit(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeATRCommit(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRCommit(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterATRCommit(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocCommitted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeDocCommitted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeRemovingDocDuringStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeRemovingDocDuringStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeRollbackDeleteInserted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeRollbackDeleteInserted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocCommittedBeforeSavingCAS(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterDocCommittedBeforeSavingCAS(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocCommitted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterDocCommitted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedRemove(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeStagedRemove(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedReplace(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeStagedReplace(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocRemoved(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeDocRemoved(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocRolledBack(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeDocRolledBack(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocRemovedPreRetry(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterDocRemovedPreRetry(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocRemovedPostRetry(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterDocRemovedPostRetry(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterGetComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterGetComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterStagedReplaceComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterStagedReplaceComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterStagedRemoveComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterStagedRemoveComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterStagedInsertComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterStagedInsertComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterRollbackReplaceOrRemove(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterRollbackReplaceOrRemove(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterRollbackDeleteInserted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterRollbackDeleteInserted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeCheckATREntryForBlockingDoc(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeCheckATREntryForBlockingDoc(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocGet(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeDocGet(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeGetDocInExistsDuringStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeGetDocInExistsDuringStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocsCommitted(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterDocsCommitted(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocsRemoved(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterDocsRemoved(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRPending(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterATRPending(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRPending(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeATRPending(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRComplete(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeATRComplete(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRRolledBack(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeATRRolledBack(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRComplete(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterATRComplete(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRAborted(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeATRAborted(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRAborted(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterATRAborted(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRRolledBack(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.AfterATRRolledBack(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRCommitAmbiguityResolution(cb func(err error)) {
	go func() {
		cb(cthw.Hooks.BeforeATRCommitAmbiguityResolution(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) RandomATRIDForVbucket(vbID []byte, cb func(string, error)) {
	go func() {
		cb(cthw.Hooks.RandomATRIDForVbucket(cthw.ctx, string(vbID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) HasExpiredClientSideHook(stage string, vbID []byte, cb func(bool, error)) {
	go func() {
		cb(cthw.Hooks.HasExpiredClientSideHook(cthw.ctx, stage, string(vbID)))
	}()
}

type noopHooksWrapper struct {
	coretxns.DefaultHooks
}

func (nhw *noopHooksWrapper) SetAttemptContext(ctx AttemptContext) {
}
