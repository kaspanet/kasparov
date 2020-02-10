package dbaccess

import (
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/httpserverutils"
)

// Context is an interface type representing the context in which queries run, currently relating to the
// existence or non-existence of a database transaction
// Call `.NoTx()` or `.NewTx()` to acquire a Context
type Context interface {
	db() (*gorm.DB, error)
}

type noTxContext struct{}

func (*noTxContext) db() (*gorm.DB, error) {
	return database.DB()
}

// TxContext represents a database context with an attached database transaction
type TxContext struct{ dbInstance *gorm.DB }

func (ctx *TxContext) db() (*gorm.DB, error) {
	return ctx.dbInstance, nil
}

// Commit commits the transaction attached to this TxContext
func (ctx *TxContext) Commit() error {
	dbErrors := ctx.dbInstance.Commit().GetErrors()

	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("some errors were encountered when commiting transaction:", dbErrors)
	}
	return nil
}

// Rollback rolls-back the transaction attached to this TxContext
func (ctx *TxContext) Rollback() error {
	dbErrors := ctx.dbInstance.Rollback().GetErrors()

	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("some errors were encountered when rolling-back transaction:", dbErrors)
	}
	return nil
}

// RollbackUnlessCommitted rolls-back the transaction atttached to this TxContext
// if it has not yet been committed.
func (ctx *TxContext) RollbackUnlessCommitted() error {
	dbErrors := ctx.dbInstance.RollbackUnlessCommitted().GetErrors()

	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("some errors were encountered when rolling-back transaction:", dbErrors)
	}
	return nil
}

var noTxContextSingleton = &noTxContext{}

// NoTx creates and returns an instance of dbaccess.Context without an attached database transaction
func NoTx() Context {
	return noTxContextSingleton
}

// NewTx returns an instance of TxContext with a new database transaction
func NewTx() (*TxContext, error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}

	return &TxContext{dbInstance: db.Begin()}, nil
}
