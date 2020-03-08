package database

import (
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
)

// DBTx is an interface type implemented both by pg.DB and pg.Tx.
// We use DBTx to execute db operations with or without transaction context.
type DBTx interface {
	Model(model ...interface{}) *orm.Query
	Select(model interface{}) error
	Insert(model ...interface{}) error
	Update(model interface{}) error
	Delete(model interface{}) error
}

// Context is an interface type representing the context in which queries run, currently relating to the
// existence or non-existence of a database transaction
// Call `.NoTx()` or `.NewTx()` to acquire a Context
type Context interface {
	DB() (DBTx, error)
}

type noTxContext struct{}

// Db returns a db instance
func (*noTxContext) DB() (DBTx, error) {
	return DB()
}

// TxContext represents a database context with an attached database transaction
type TxContext struct {
	tx        *pg.Tx
	committed bool
}

// Db returns a db instance
func (ctx *TxContext) DB() (DBTx, error) {
	return ctx.tx, nil
}

// Commit commits the transaction attached to this TxContext
func (ctx *TxContext) Commit() error {
	ctx.committed = true
	return ctx.tx.Commit()
}

// Rollback rolls-back the transaction attached to this TxContext
func (ctx *TxContext) Rollback() error {
	return ctx.tx.Rollback()
}

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
	db, err := DB()
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	return &TxContext{tx: tx}, nil
}
