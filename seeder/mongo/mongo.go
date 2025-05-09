package mongo

import (
	"context"
	"errors"
	"github.com/lefalya/pageflow"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"time"
)

var (
	NoDatabaseProvided           = errors.New("No database provided!")
	DocumentOrReferencesNotFound = errors.New("Document or References not found!")
)

type PaginateMongoSeeder[T pageflow.MongoItemBlueprint] struct {
	coll             *mongo.Collection
	baseClient       *pageflow.Base[T]
	paginationClient *pageflow.Paginate[T]
	scoringField     string
}

func (m *PaginateMongoSeeder[T]) FindOne(key string, value string, initItem func() T) (T, error) {
	mongoItem := initItem()
	if m.coll == nil {
		return mongoItem, NoDatabaseProvided
	}

	filter := bson.D{{key, value}}
	err := m.coll.FindOne(context.TODO(), filter).Decode(&mongoItem)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return mongoItem, DocumentOrReferencesNotFound
		}
		return mongoItem, err
	}

	return mongoItem, nil
}

func (m *PaginateMongoSeeder[T]) SeedOne(key string, value string, initItem func() T) error {
	item, err := m.FindOne(key, value, initItem)
	if err != nil {
		return err
	}

	return m.baseClient.Set(item)
}

func (m *PaginateMongoSeeder[T]) SeedPartial(subtraction int64, validLastRandId string, query bson.D, paginateParams []string, initItem func() T) error {
	var cursor *mongo.Cursor
	var reference T
	var withReference bool
	var err error
	var firstPage bool
	var filter bson.D
	var errorDecode error
	var compOp string
	var sortField string

	// If scoringField is specified, we'll use it for sorting instead of _id
	if m.scoringField != "" {
		sortField = m.scoringField
	} else {
		sortField = "_id" // Default sort by _id
	}

	if query == nil {
		query = bson.D{}
	}

	findOptions := options.Find()
	if m.paginationClient.GetDirection() == pageflow.Ascending {
		findOptions.SetSort(bson.D{{sortField, 1}})
		compOp = "$gt"
	} else {
		findOptions.SetSort(bson.D{{sortField, -1}})
		compOp = "$lt"
	}

	if validLastRandId != "" {
		reference, err = m.FindOne("randid", validLastRandId, initItem)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return DocumentOrReferencesNotFound
			}
			return err
		} else {
			withReference = true
		}

	} else {
		firstPage = true
	}

	if withReference {
		var limit int64
		if subtraction > 0 {
			limit = int64(m.paginationClient.GetItemPerPage()) - subtraction
		} else {
			limit = int64(m.paginationClient.GetItemPerPage())
		}

		findOptions.SetLimit(limit)

		var comparisonValue interface{}
		if m.scoringField != "" {
			comparisonValue = getFieldValue(reference, m.scoringField)
		} else {
			comparisonValue = reference.GetObjectID()
		}

		filter = bson.D{
			{"$and",
				bson.A{
					query,
					bson.D{
						{sortField, bson.D{{compOp, comparisonValue}}},
					},
				},
			},
		}
	} else {
		filter = query
		findOptions.SetLimit(m.paginationClient.GetItemPerPage())
	}

	cursor, err = m.coll.Find(context.TODO(), filter, findOptions)
	if err != nil {
		return err
	}
	defer cursor.Close(context.TODO())

	var counterLoop int64
	counterLoop = 0
	for cursor.Next(context.TODO()) {
		item := initItem()
		errorDecode = cursor.Decode(&item)
		if errorDecode != nil {
			continue
		}

		m.baseClient.Set(item)
		m.paginationClient.IngestItem(item, paginateParams, true)
		counterLoop++
	}

	if firstPage && counterLoop == 0 {
		m.paginationClient.SetBlankPage(paginateParams)
	} else if firstPage && counterLoop > 0 && counterLoop < m.paginationClient.GetItemPerPage() {
		m.paginationClient.SetFirstPage(paginateParams)
	} else if validLastRandId != "" && counterLoop < m.paginationClient.GetItemPerPage() {
		m.paginationClient.SetLastPage(paginateParams)
	}

	return nil
}

func (m *PaginateMongoSeeder[T]) SeedAll(query bson.D, listParam []string, initItem func() T) error {
	cursor, err := m.coll.Find(context.TODO(), query)
	if err != nil {
		return err
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		item := initItem()
		errorDecode := cursor.Decode(&item)
		if errorDecode != nil {
			continue
		}

		m.baseClient.Set(item)
		m.paginationClient.IngestItem(item, listParam, true)
	}

	return nil
}

type SortedMongoSeeder[T pageflow.MongoItemBlueprint] struct {
	coll         *mongo.Collection
	baseClient   *pageflow.Base[T]
	sortedClient *pageflow.Sorted[T]
	scoringField string
}

func (s *SortedMongoSeeder[T]) Seed(query bson.D, listParam []string, initItem func() T) error {
	cursor, err := s.coll.Find(context.TODO(), query)
	if err != nil {
		return err
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		item := initItem()
		errorDecode := cursor.Decode(&item)
		if errorDecode != nil {
			continue
		}

		s.baseClient.Set(item)
		s.sortedClient.IngestItem(item, listParam, true)
	}

	return nil
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

func NewPaginateMongoSeederWithReference[T pageflow.MongoItemBlueprint](coll *mongo.Collection, baseClient *pageflow.Base[T], paginateClient *pageflow.Paginate[T], sortingReference string) *PaginateMongoSeeder[T] {
	return &PaginateMongoSeeder[T]{
		coll:             coll,
		baseClient:       baseClient,
		paginationClient: paginateClient,
		scoringField:     sortingReference,
	}
}

func NewPaginateMongoSeeder[T pageflow.MongoItemBlueprint](coll *mongo.Collection, baseClient *pageflow.Base[T], paginateClient *pageflow.Paginate[T]) *PaginateMongoSeeder[T] {
	return &PaginateMongoSeeder[T]{
		coll:             coll,
		baseClient:       baseClient,
		paginationClient: paginateClient,
	}
}

func NewSortedMongoSeederWithReference[T pageflow.MongoItemBlueprint](coll *mongo.Collection, baseClient *pageflow.Base[T], sortedClient *pageflow.Sorted[T], sortingReference string) *SortedMongoSeeder[T] {
	return &SortedMongoSeeder[T]{
		coll:         coll,
		baseClient:   baseClient,
		sortedClient: sortedClient,
		scoringField: sortingReference,
	}
}

func NewSortedMongoSeeder[T pageflow.MongoItemBlueprint](coll *mongo.Collection, baseClient *pageflow.Base[T], sortedClient *pageflow.Sorted[T]) *SortedMongoSeeder[T] {
	return &SortedMongoSeeder[T]{
		coll:         coll,
		baseClient:   baseClient,
		sortedClient: sortedClient,
	}
}
