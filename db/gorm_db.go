package db

import (
	"errors"
	gormTracer "gopkg.in/DataDog/dd-trace-go.v1/contrib/gorm.io/gorm.v1"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"log"
	"os"
	"strconv"
	"strings"
)

const DdTraceEnabledEnv = "DD_TRACE_ENABLED"

// TypeDb is a type of database
type TypeDb string

const (
	Postgres TypeDb = "postgres"
	Mysql    TypeDb = "mysql"
)

// isTracingEnabled checks if tracing is enabled
func isTracingEnabled() bool {
	traceEnabled, _ := strconv.ParseBool(os.Getenv(DdTraceEnabledEnv))
	return traceEnabled
}

// NewDB creates a new database connection
func NewDB(dbType TypeDb, connString string, debug bool, traceServiceName string) (*gorm.DB, error) {
	if strings.TrimSpace(connString) == "" {
		return nil, errors.New("db: missing connection string")
	}

	dialect, err := GetDialect(dbType, connString)
	if err != nil {
		return nil, err
	}

	config := &gorm.Config{}
	if debug {
		config.Logger = gormLogger.Default.LogMode(gormLogger.Info)
	}

	if isTracingEnabled() {
		return gormTracer.Open(dialect, config, gormTracer.WithServiceName(traceServiceName))
	}

	return gorm.Open(dialect, config)
}

// GetDialect returns a dialect for a given database type
func GetDialect(typeDb TypeDb, connString string) (gorm.Dialector, error) {
	switch typeDb {
	case Postgres:
		return postgres.Open(connString), nil
	case Mysql:
		return mysql.Open(connString), nil
	default:
		return nil, errors.New("db: unsupported database type")
	}
}

// CloseDBConnection closes a database connection
func CloseDBConnection(dbConn *gorm.DB, logger *log.Logger) {
	sqlDB, err := dbConn.DB()
	if err != nil {
		logger.Printf("failed to get sqlDB from Gorm connection: %ы", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logger.Printf("failed to close database connection: %ы", err)
	}
}
