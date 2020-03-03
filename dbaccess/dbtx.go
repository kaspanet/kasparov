package dbaccess

import (
	"github.com/go-pg/pg/v9"
	"github.com/kaspanet/kasparov/database"
)

// Context is an interface type representing the context in which queries run, currently relating to the
// existence or non-existence of a database transaction
// Call `.NoTx()` or `.NewTx()` to acquire a Context
type Context interface {
	db() (*pg.DB, error)
}

type noTxContext struct{}

func (*noTxContext) db() (*pg.DB, error) {
	return database.DB()
}

// TxContext represents a database context with an attached database transaction
type TxContext struct{ dbInstance *pg.DB }

func (ctx *TxContext) db() (*pg.DB, error) {
	return ctx.dbInstance, nil
}

// Commit commits the transaction attached to this TxContext
func (ctx *TxContext) Commit() error {
	tx, err := ctx.dbInstance.Begin()
	if err != nil {
		return err
	}
	return tx.Commit()

}

// Rollback rolls-back the transaction attached to this TxContext
func (ctx *TxContext) Rollback() error {
	tx, err := ctx.dbInstance.Begin()
	if err != nil {
		return err
	}
	return tx.Rollback()
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
	return &TxContext{dbInstance: db}, nil
}
