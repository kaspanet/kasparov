package database

import (
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/pkg/errors"
)

// DB is an interface type implemented both by pg.DB and pg.Tx.
// We use DB to execute db operations with or without transaction context.
type DB interface {
	Model(model ...interface{}) *orm.Query
	Select(model interface{}) error
	Insert(model ...interface{}) error
	Update(model interface{}) error
	Delete(model interface{}) error
	QueryOne(model, query interface{}, params ...interface{}) (orm.Result, error)
}

// Context is an interface type representing the context in which queries run, currently relating to the
// existence or non-existence of a database transaction
// Call `.NoTx()` or `.NewTx()` to acquire a Context
type Context interface {
	DB() (DB, error)
}

type noTxContext struct{}

// DB returns a db instance
func (*noTxContext) DB() (DB, error) {
	return DBInstance()
}

// TxContext represents a database context with an attached database transaction
type TxContext struct {
	tx        *pg.Tx
	committed bool
}

// DB returns a db instance
func (ctx *TxContext) DB() (DB, error) {
	return ctx.tx, nil
}

// Commit commits the transaction attached to this TxContext
func (ctx *TxContext) Commit() error {
	ctx.committed = true
	return ctx.tx.Commit()
}

// Rollback rolls-back the transaction attached to this TxContext.
func (ctx *TxContext) Rollback() error {
	if ctx.committed {
		return errors.Errorf("cannot rollback a committed transaction")
	}
	return ctx.tx.Rollback()
}

// RollbackUnlessCommitted rolls-back the transaction attached to this TxContext if the transaction was not committed.
func (ctx *TxContext) RollbackUnlessCommitted() error {
	if !ctx.committed {
		return ctx.Rollback()
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
	db, err := DBInstance()
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	return &TxContext{tx: tx}, nil
}
