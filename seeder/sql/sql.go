package sql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/lefalya/pageflow"
	"reflect"
	"time"
)

var (
	NoDatabaseProvided           = errors.New("No database provided!")
	DocumentOrReferencesNotFound = errors.New("Document or References not found!")
	QueryOrScannerNotConfigured  = errors.New("Required queries or scanner not configured")
	NilConfiguration             = errors.New("No configuration found!")
)

type RowScanner[T pageflow.SQLItemBlueprint] func(row *sql.Row) (T, error)

type RowsScanner[T pageflow.SQLItemBlueprint] func(rows *sql.Rows) (T, error)

type SQLSeederConfig[T pageflow.SQLItemBlueprint] struct {
	RowQuery       string
	RowScanner     RowScanner[T]
	SelectAllQuery string
	FirstPageQuery string
	NextPageQuery  string
	RowsScanner    RowsScanner[T]
}

type PaginateSQLSeeder[T pageflow.SQLItemBlueprint] struct {
	db               *sql.DB
	baseClient       *pageflow.Base[T]
	paginationClient *pageflow.Paginate[T]
	config           *SQLSeederConfig[T]
	scoringField     string
}

func (s *PaginateSQLSeeder[T]) FindOne(queryArgs []interface{}) (T, error) {
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

func (s *PaginateSQLSeeder[T]) SeedOne(queryArgs []interface{}) error {
	item, err := s.FindOne(queryArgs)
	if err != nil {
		return err
	}

	return s.baseClient.Set(item)
}

func (s *PaginateSQLSeeder[T]) SeedPartial(
	queryArgs []interface{},
	lastRandId string,
	paginateParams []string,
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
		reference, err := s.FindOne([]interface{}{lastRandId})
		if err != nil {
			return DocumentOrReferencesNotFound
		} else {
			firstPage = false
			queryToUse = s.config.NextPageQuery

			if s.scoringField != "" {
				queryArgs = append(queryArgs, getFieldValue(reference, s.scoringField))
			} else {
				queryArgs = append(queryArgs, reference.GetCreatedAt())
			}
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
		s.paginationClient.IngestItem(item, paginateParams, true)
		counterLoop++
	}

	if firstPage && counterLoop == 0 {
		s.paginationClient.SetBlankPage(paginateParams)
	} else if firstPage && counterLoop > 0 && counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetFirstPage(paginateParams)
	} else if !firstPage && counterLoop < s.paginationClient.GetItemPerPage() {
		s.paginationClient.SetLastPage(paginateParams)
	}

	return nil
}

func NewPaginateSQLSeeder[T pageflow.SQLItemBlueprint](
	db *sql.DB,
	baseClient *pageflow.Base[T],
	paginateClient *pageflow.Paginate[T],
	config *SQLSeederConfig[T],
) *PaginateSQLSeeder[T] {
	return &PaginateSQLSeeder[T]{
		db:               db,
		baseClient:       baseClient,
		paginationClient: paginateClient,
		config:           config,
	}
}

type SortedSQLSeeder[T pageflow.SQLItemBlueprint] struct {
	db           *sql.DB
	baseClient   *pageflow.Base[T]
	sortedClient *pageflow.Sorted[T]
	config       *SQLSeederConfig[T]
	scoringField string
}

func (s *SortedSQLSeeder[T]) SeedAll(
	args []interface{},
	param []string,
) error {
	if s.db == nil {
		return NoDatabaseProvided
	}

	if s.config.RowsScanner == nil {
		return QueryOrScannerNotConfigured
	}

	rows, err := s.db.QueryContext(context.TODO(), s.config.SelectAllQuery, args...)
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
		s.sortedClient.IngestItem(item, param, true)
	}

	return nil
}

func NewSortedSQLSeeder[T pageflow.SQLItemBlueprint](
	db *sql.DB,
	baseClient *pageflow.Base[T],
	sortedClient *pageflow.Sorted[T],
	config *SQLSeederConfig[T],
) *SortedSQLSeeder[T] {
	return &SortedSQLSeeder[T]{
		db:           db,
		baseClient:   baseClient,
		sortedClient: sortedClient,
		config:       config,
	}
}

func getFieldValue(obj interface{}, fieldName string) interface{} {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return time.Time{}
	}

	field := val.FieldByName(fieldName)
	if !field.IsValid() {
		return time.Time{}
	}

	return field.Interface()
}
