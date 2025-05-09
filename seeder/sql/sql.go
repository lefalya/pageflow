package sql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/lefalya/pageflow"
)

var (
	NoDatabaseProvided           = errors.New("No database provided!")
	DocumentOrReferencesNotFound = errors.New("Document or References not found!")
	QueryOrScannerNotConfigured  = errors.New("Required queries or scanner not configured")
	NilConfiguration             = errors.New("No configuration found!")
)

type RowScanner[T pageflow.MongoItemBlueprint] func(row *sql.Row) (T, error)

type RowsScanner[T pageflow.MongoItemBlueprint] func(rows *sql.Rows) (T, error)

type SQLSeederConfig[T pageflow.MongoItemBlueprint] struct {
	RowQuery        string
	RowScanner      RowScanner[T]
	SelectManyQuery string
	FirstPageQuery  string
	NextPageQuery   string
	RowsScanner     RowsScanner[T]
}

type SQLSeeder[T pageflow.MongoItemBlueprint] struct {
	db               *sql.DB
	baseClient       *pageflow.Base[T]
	paginationClient *pageflow.Paginate[T]
	config           *SQLSeederConfig[T]
}

func (s *SQLSeeder[T]) FindOne(queryArgs []interface{}) (T, error) {
	var item T
	if s.db == nil {
		return item, NoDatabaseProvided
	}

	if s.config.RowQuery == "" || s.config.RowScanner == nil {
		return item, QueryOrScannerNotConfigured
	}

	row := s.db.QueryRowContext(context.TODO(), s.config.RowQuery, queryArgs...)

	item, err := s.config.RowScanner(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return item, DocumentOrReferencesNotFound
		}
		return item, err
	}

	return item, nil
}

func (s *SQLSeeder[T]) SeedOne(queryArgs []interface{}) error {
	item, err := s.FindOne(queryArgs)
	if err != nil {
		return err
	}

	return s.baseClient.Set(item)
}

func (s *SQLSeeder[T]) SeedPartial(
	queryArgs []interface{},
	lastRandId string,
	paginateKey string,
) error {
	var firstPage bool
	var queryToUse string

	if s.db == nil {
		return NoDatabaseProvided
	}

	if s.config == nil {
		return NilConfiguration
	}

	if lastRandId == "" {
		firstPage = true
		queryToUse = s.config.FirstPageQuery
	} else {
		_, err := s.FindOne([]interface{}{lastRandId})
		if err != nil {
			return DocumentOrReferencesNotFound
		} else {
			firstPage = false
			queryToUse = s.config.NextPageQuery
		}
	}

	rows, err := s.db.QueryContext(context.TODO(), queryToUse, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var counterLoop int64 = 0
	for rows.Next() {
		item, err := s.config.RowsScanner(rows)
		if err != nil {
			continue
		}

		s.baseClient.Set(item)
		s.paginationClient.AddItem(item, []string{paginateKey}, true)
		counterLoop++
	}

	if firstPage && counterLoop == 0 {
		s.paginationClient.SetBlankPage([]string{paginateKey})
	} else if firstPage && counterLoop > 0 && counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetFirstPage([]string{paginateKey})
	} else if !firstPage && counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetLastPage([]string{paginateKey})
	}

	return nil
}

func (s *SQLSeeder[T]) SeedAll(
	args []interface{},
	listKey string,
) error {
	if s.db == nil {
		return NoDatabaseProvided
	}

	if s.config.RowsScanner == nil {
		return QueryOrScannerNotConfigured
	}

	rows, err := s.db.QueryContext(context.TODO(), s.config.SelectManyQuery, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		item, err := s.config.RowsScanner(rows)
		if err != nil {
			continue
		}

		s.baseClient.Set(item)
		s.paginationClient.AddItem(item, []string{listKey}, true)
	}

	return nil
}

func NewSQLSeeder[T pageflow.MongoItemBlueprint](
	db *sql.DB,
	baseClient *pageflow.Base[T],
	paginateClient *pageflow.Paginate[T],
	config *SQLSeederConfig[T],
) *SQLSeeder[T] {
	return &SQLSeeder[T]{
		db:               db,
		baseClient:       baseClient,
		paginationClient: paginateClient,
		config:           config,
	}
}
