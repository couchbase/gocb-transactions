package transactions

import (
	coretxns "github.com/couchbaselabs/gocbcore-transactions"
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
