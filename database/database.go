package database

import (
	nativeerrors "errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/jinzhu/gorm"
	"github.com/kaspanet/kasparov/config"
	"github.com/kaspanet/kasparov/httpserverutils"
	"github.com/pkg/errors"
	"os"

	"github.com/golang-migrate/migrate/v4"
)

// db is the Kasparov database.
var db *gorm.DB

const (
	utcTimeZone = "UTC"
)

// DB returns a reference to the database connection
func DB() (*gorm.DB, error) {
	if db == nil {
		return nil, errors.New("Database is not connected")
	}
	return db, nil
}

type gormLogger struct{}

func (l gormLogger) Print(v ...interface{}) {
	str := fmt.Sprint(v...)
	log.Errorf(str)
}

// Connect connects to the database mentioned in
// config variable.
func Connect(cfg *config.KasparovFlags) error {
	migrator, driver, err := openMigrator(cfg)
	if err != nil {
		return err
	}
	isCurrent, version, err := isCurrent(migrator, driver)
	if err != nil {
		return errors.Errorf("Error checking whether the database is current: %s", err)
	}
	if !isCurrent {
		return errors.Errorf("Database is not current (version %d). Please migrate"+
			" the database by running the server with --migrate flag and then run it again.", version)
	}
	db, err = gorm.Open("postgres", buildConnectionString(cfg))
	if err != nil {
		return err
	}
	db.SetLogger(gormLogger{})

	return validateTimeZone(db)
}

func validateTimeZone(db *gorm.DB) error {
	var timeZone string
	dbResult := db.
		Raw("SELECT current_setting('TIMEZONE') as time_zone")
	dbResult.Row().Scan(&timeZone)
	dbErrors := dbResult.GetErrors()
	if httpserverutils.HasDBError(dbErrors) {
		return httpserverutils.NewErrorFromDBErrors("some errors were encountered when "+
			"checking the database timezone:", dbErrors)
	}

	if timeZone != utcTimeZone {
		return errors.Errorf("to prevent conversion errors - Kasparov should only run with "+
			"a database configured to use the UTC timezone, currently configured timezone is %s", timeZone)
	}
	return nil
}

// Close closes the connection to the database
func Close() error {
	if db == nil {
		return nil
	}
	err := db.Close()
	db = nil
	return err
}

func buildMigratorConnectionString(cfg *config.KasparovFlags) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBSSLMode)
}

func buildConnectionString(cfg *config.KasparovFlags) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSSLMode)
}

// isCurrent resolves whether the database is on the latest
// version of the schema.
func isCurrent(migrator *migrate.Migrate, driver source.Driver) (bool, uint, error) {
	// Get the current version
	version, isDirty, err := migrator.Version()
	if nativeerrors.Is(err, migrate.ErrNilVersion) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, errors.WithStack(err)
	}
	if isDirty {
		return false, 0, errors.Errorf("Database is dirty")
	}

	// The database is current if Next returns ErrNotExist
	_, err = driver.Next(version)
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		if pathErr.Err == os.ErrNotExist {
			return true, version, nil
		}
	}
	return false, version, err
}

func openMigrator(cfg *config.KasparovFlags) (*migrate.Migrate, source.Driver, error) {
	driver, err := source.Open("file://../database/migrations")
	if err != nil {
		return nil, nil, err
	}
	migrator, err := migrate.NewWithSourceInstance(
		"migrations", driver, buildMigratorConnectionString(cfg))
	if err != nil {
		return nil, nil, err
	}
	return migrator, driver, nil
}

// Migrate database to the latest version.
func Migrate(cfg *config.KasparovFlags) error {
	migrator, driver, err := openMigrator(cfg)
	if err != nil {
		return err
	}
	isCurrent, version, err := isCurrent(migrator, driver)
	if err != nil {
		return errors.Errorf("Error checking whether the database is current: %s", err)
	}
	if isCurrent {
		log.Infof("Database is already up-to-date (version %d)", version)
		return nil
	}
	err = migrator.Up()
	if err != nil {
		return err
	}
	version, isDirty, err := migrator.Version()
	if err != nil {
		return err
	}
	if isDirty {
		return errors.Errorf("error migrating database: database is dirty")
	}
	log.Infof("Migrated database to the latest version (version %d)", version)
	return nil
}
