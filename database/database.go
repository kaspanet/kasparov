package database

import (
	nativeerrors "errors"
	"fmt"
	"github.com/go-pg/pg/v9"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/kaspanet/kasparov/config"
	"github.com/pkg/errors"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
)

// db is the Kasparov database.
var db *pg.DB

var (
	allowedTimeZones = map[string]struct{}{
		"UTC":     {},
		"Etc/UTC": {},
	}
)

// DBInstance returns a reference to the database connection
func DBInstance() (*pg.DB, error) {
	if db == nil {
		return nil, errors.New("Database is not connected")
	}
	return db, nil
}

// Connect connects to the database mentioned in the config variable.
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
			" the database by running the server with --migrate flag and then run it again", version)
	}

	connectionOptions, err := pg.ParseURL(buildConnectionString(cfg))
	if err != nil {
		return err
	}

	db = pg.Connect(connectionOptions)

	return validateTimeZone(db)
}

func validateTimeZone(db *pg.DB) error {
	var timeZone string
	_, err := db.
		QueryOne(pg.Scan(&timeZone), `SELECT current_setting('TIMEZONE') as time_zone`)

	if err != nil {
		return errors.WithMessage(err, "some errors were encountered when "+
			"checking the database timezone:")
	}

	if _, ok := allowedTimeZones[timeZone]; !ok {
		return errors.Errorf("to prevent conversion errors - Kasparov should only run with "+
			"a database configured to use one of the allowed timezone. Currently configured timezone "+
			"is %s. Allowed time zones: %s", timeZone, allowedTimezonesString())
	}
	return nil
}

func allowedTimezonesString() string {
	keys := make([]string, 0, len(allowedTimeZones))
	for allowedTimeZone := range allowedTimeZones {
		keys = append(keys, fmt.Sprintf("'%s'", allowedTimeZone))
	}
	return strings.Join(keys, ", ")
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

func buildConnectionString(cfg *config.KasparovFlags) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		cfg.DBUser, cfg.DBPassword, cfg.DBAddress, cfg.DBName, cfg.DBSSLMode)
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
		"migrations", driver, buildConnectionString(cfg))
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
