package database

import (
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
)

// DbTx is an interface type implemented both by pg.DB and pg.Tx.
// We use DbTX to execute db operations with or without transaction context.
type DbTx interface {
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
	db() (DbTx, error)
}

type noTxContext struct{}

func (*noTxContext) db() (DbTx, error) {
	return DB()
}

// TxContext represents a database context with an attached database transaction
type TxContext struct{ tx *pg.Tx }

func (ctx *TxContext) db() (DbTx, error) {
	return ctx.tx, nil
}

// Commit commits the transaction attached to this TxContext
func (ctx *TxContext) Commit() error {
	return ctx.tx.Commit()
}

// Rollback rolls-back the transaction attached to this TxContext
func (ctx *TxContext) Rollback() error {
	return ctx.tx.Rollback()
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
