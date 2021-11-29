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
	BeforeRemoveStagedInsert(ctx AttemptContext, docID string) error
	AfterRemoveStagedInsert(ctx AttemptContext, docID string) error
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
	RandomATRIDForVbucket(ctx AttemptContext) (string, error)
	HasExpiredClientSideHook(ctx AttemptContext, stage string, vbID string) (bool, error)
	BeforeQuery(ctx AttemptContext, statement string) error
	AfterQuery(ctx AttemptContext, statement string) error
}

// CleanupHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type CleanupHooks interface {
	BeforeATRGet(id string) error
	BeforeDocGet(id string) error
	BeforeRemoveLinks(id string) error
	BeforeCommitDoc(id string) error
	BeforeRemoveDocStagedForRemoval(id string) error
	BeforeRemoveDoc(id string) error
	BeforeATRRemove(id string) error
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

type hooksWrapper interface {
	SetAttemptContext(ctx AttemptContext)
	coretxns.TransactionHooks
	Hooks() TransactionHooks
}

type cleanupHooksWrapper interface {
	coretxns.CleanUpHooks
}

type coreTxnsHooksWrapper struct {
	ctx   AttemptContext
	hooks TransactionHooks
}

type clientRecordHooksWrapper interface {
	coretxns.ClientRecordHooks
}

func (cthw *coreTxnsHooksWrapper) SetAttemptContext(ctx AttemptContext) {
	cthw.ctx = ctx
}

func (cthw *coreTxnsHooksWrapper) Hooks() TransactionHooks {
	return cthw.hooks
}

func (cthw *coreTxnsHooksWrapper) BeforeATRCommit(cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeATRCommit(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRCommit(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterATRCommit(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocCommitted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeDocCommitted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeRemovingDocDuringStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeRemovingDocDuringStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeRollbackDeleteInserted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeRollbackDeleteInserted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocCommittedBeforeSavingCAS(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterDocCommittedBeforeSavingCAS(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocCommitted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterDocCommitted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedRemove(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeStagedRemove(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeStagedReplace(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeStagedReplace(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocRemoved(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeDocRemoved(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocRolledBack(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeDocRolledBack(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocRemovedPreRetry(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterDocRemovedPreRetry(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocRemovedPostRetry(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterDocRemovedPostRetry(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterGetComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterGetComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterStagedReplaceComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterStagedReplaceComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterStagedRemoveComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterStagedRemoveComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterStagedInsertComplete(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterStagedInsertComplete(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterRollbackReplaceOrRemove(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterRollbackReplaceOrRemove(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterRollbackDeleteInserted(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterRollbackDeleteInserted(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeCheckATREntryForBlockingDoc(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeCheckATREntryForBlockingDoc(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeDocGet(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeDocGet(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeGetDocInExistsDuringStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeGetDocInExistsDuringStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeRemoveStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeRemoveStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterRemoveStagedInsert(docID []byte, cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterRemoveStagedInsert(cthw.ctx, string(docID)))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocsCommitted(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterDocsCommitted(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterDocsRemoved(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterDocsRemoved(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRPending(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterATRPending(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRPending(cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeATRPending(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRComplete(cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeATRComplete(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRRolledBack(cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeATRRolledBack(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRComplete(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterATRComplete(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRAborted(cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeATRAborted(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRAborted(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterATRAborted(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) AfterATRRolledBack(cb func(err error)) {
	go func() {
		cb(cthw.hooks.AfterATRRolledBack(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) BeforeATRCommitAmbiguityResolution(cb func(err error)) {
	go func() {
		cb(cthw.hooks.BeforeATRCommitAmbiguityResolution(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) RandomATRIDForVbucket(cb func(string, error)) {
	go func() {
		cb(cthw.hooks.RandomATRIDForVbucket(cthw.ctx))
	}()
}

func (cthw *coreTxnsHooksWrapper) HasExpiredClientSideHook(stage string, vbID []byte, cb func(bool, error)) {
	go func() {
		cb(cthw.hooks.HasExpiredClientSideHook(cthw.ctx, stage, string(vbID)))
	}()
}

type coreTxnsCleanupHooksWrapper struct {
	ctx          AttemptContext
	CleanupHooks CleanupHooks
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeATRGet(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeATRGet(string(id)))
	}()
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeDocGet(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeDocGet(string(id)))
	}()
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeRemoveLinks(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeRemoveLinks(string(id)))
	}()
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeCommitDoc(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeCommitDoc(string(id)))
	}()
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeRemoveDocStagedForRemoval(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeRemoveDocStagedForRemoval(string(id)))
	}()
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeRemoveDoc(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeRemoveDoc(string(id)))
	}()
}

func (cthw *coreTxnsCleanupHooksWrapper) BeforeATRRemove(id []byte, cb func(error)) {
	go func() {
		cb(cthw.CleanupHooks.BeforeATRRemove(string(id)))
	}()
}

type coreTxnsClientRecordHooksWrapper struct {
	coreTxnsCleanupHooksWrapper
	ClientRecordHooks ClientRecordHooks
}

func (hw *coreTxnsClientRecordHooksWrapper) BeforeCreateRecord(cb func(error)) {
	go func() {
		cb(hw.ClientRecordHooks.BeforeCreateRecord())
	}()
}

func (hw *coreTxnsClientRecordHooksWrapper) BeforeRemoveClient(cb func(error)) {
	go func() {
		cb(hw.ClientRecordHooks.BeforeRemoveClient())
	}()
}

func (hw *coreTxnsClientRecordHooksWrapper) BeforeUpdateCAS(cb func(error)) {
	go func() {
		cb(hw.ClientRecordHooks.BeforeUpdateCAS())
	}()
}

func (hw *coreTxnsClientRecordHooksWrapper) BeforeGetRecord(cb func(error)) {
	go func() {
		cb(hw.ClientRecordHooks.BeforeGetRecord())
	}()
}

func (hw *coreTxnsClientRecordHooksWrapper) BeforeUpdateRecord(cb func(error)) {
	go func() {
		cb(hw.ClientRecordHooks.BeforeUpdateRecord())
	}()
}

type noopHooksWrapper struct {
	coretxns.DefaultHooks
	hooks defaultHooks
}

func (nhw *noopHooksWrapper) SetAttemptContext(ctx AttemptContext) {
}

func (nhw *noopHooksWrapper) Hooks() TransactionHooks {
	return nhw.hooks
}

type noopCleanupHooksWrapper struct {
	coretxns.DefaultCleanupHooks
}

type noopClientRecordHooksWrapper struct {
	coretxns.DefaultCleanupHooks
	coretxns.DefaultClientRecordHooks
}

type defaultHooks struct {
}

func (d defaultHooks) BeforeATRCommit(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) AfterATRCommit(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) BeforeDocCommitted(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeRemovingDocDuringStagedInsert(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeRollbackDeleteInserted(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterDocCommittedBeforeSavingCAS(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterDocCommitted(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeStagedInsert(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeStagedRemove(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeStagedReplace(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeDocRemoved(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeDocRolledBack(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterDocRemovedPreRetry(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterDocRemovedPostRetry(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterGetComplete(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterStagedReplaceComplete(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterStagedRemoveComplete(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterStagedInsertComplete(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterRollbackReplaceOrRemove(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterRollbackDeleteInserted(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeCheckATREntryForBlockingDoc(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeDocGet(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeGetDocInExistsDuringStagedInsert(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) BeforeRemoveStagedInsert(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterRemoveStagedInsert(ctx AttemptContext, docID string) error {
	return nil
}

func (d defaultHooks) AfterDocsCommitted(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) AfterDocsRemoved(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) AfterATRPending(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) BeforeATRPending(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) BeforeATRComplete(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) BeforeATRRolledBack(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) AfterATRComplete(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) BeforeATRAborted(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) AfterATRAborted(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) AfterATRRolledBack(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) BeforeATRCommitAmbiguityResolution(ctx AttemptContext) error {
	return nil
}

func (d defaultHooks) RandomATRIDForVbucket(ctx AttemptContext) (string, error) {
	return "", nil
}

func (d defaultHooks) HasExpiredClientSideHook(ctx AttemptContext, stage string, vbID string) (bool, error) {
	return false, nil
}

func (d defaultHooks) BeforeQuery(ctx AttemptContext, statement string) error {
	return nil
}

func (d defaultHooks) AfterQuery(ctx AttemptContext, statement string) error {
	return nil
}
