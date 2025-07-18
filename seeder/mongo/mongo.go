package mongo

import (
	"context"
	"errors"
	"github.com/lefalya/pageflow"
	"github.com/lefalya/pageflow/helper"
	"github.com/redis/go-redis/v9"
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
	} else if validLastRandId != "" && subtraction+counterLoop < m.paginationClient.GetItemPerPage() {
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

type SortedMongoSeeder[T pageflow.MongoItemBlueprint] struct {
	coll                    *mongo.Collection
	baseClient              *pageflow.Base[T]
	sortedClient            *pageflow.Sorted[T]
	scoringField            string
	mostRecentItemOnCache   time.Time
	mostEarliestItemOnCache time.Time
}

func (s *SortedMongoSeeder[T]) SeedAll(query bson.D, keyParams []string, initItem func() T) error {
	if query == nil {
		query = bson.D{}
	}

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
		s.sortedClient.IngestItem(item, keyParams, true)
	}

	return nil
}

func (s *SortedMongoSeeder[T]) SeedByTimeRange(query bson.D, keyParams []string, seedingRanges [][]time.Time, initItem func() T) error {
	var sortField string

	if s.scoringField != "" {
		sortField = s.scoringField
	} else {
		sortField = "_id"
	}

	findOptions := options.Find()
	if s.sortedClient.GetDirection() == pageflow.Ascending {
		findOptions.SetSort(bson.D{{sortField, 1}})
	} else {
		findOptions.SetSort(bson.D{{sortField, -1}})
	}

	// Get initial cache boundaries once, outside the loop
	var isMostEarliestExists bool
	var isMostRecentExists bool
	var mostEarliestTimeOnCache *time.Time
	var mostRecentTimeOnCache *time.Time

	earliest, err := s.sortedClient.GetMostEarliestItem(keyParams)
	if err != nil {
		if err == redis.Nil {
			isMostEarliestExists = false
		} else {
			return err
		}
	} else {
		isMostEarliestExists = true
		mostEarliestTimeOnCache = earliest
	}

	recent, err := s.sortedClient.GetMostRecentItem(keyParams)
	if err != nil {
		if err == redis.Nil {
			isMostRecentExists = false
		} else {
			return err
		}
	} else {
		isMostRecentExists = true
		mostRecentTimeOnCache = recent
	}

	// Track whether we need to update Redis
	var needsEarliestUpdate bool
	var needsRecentUpdate bool

	for _, timeRange := range seedingRanges {
		if len(timeRange) != 2 {
			continue
		}

		rangeLower := timeRange[0]
		rangeUpper := timeRange[1]

		filter := bson.D{
			{"$and",
				bson.A{
					query,
					bson.D{
						{sortField, bson.D{{"$lte", rangeUpper}}},
					},
					bson.D{
						{sortField, bson.D{{"$gte", rangeLower}}},
					},
				},
			},
		}

		cursor, err := s.coll.Find(context.TODO(), filter, findOptions)
		if err != nil {
			return err
		}

		var tempEarliestTime *time.Time
		var tempLatestTime *time.Time
		counterLoop := 0

		for cursor.Next(context.TODO()) {
			item := initItem()
			errorDecode := cursor.Decode(&item)
			if errorDecode != nil {
				continue
			}

			// Use sortField consistently (FIXED)
			itemTime, errGIScore := helper.GetItemScoreAsTime(item, sortField)
			if errGIScore != nil {
				cursor.Close(context.TODO())
				return errGIScore
			}

			if tempEarliestTime == nil || itemTime.Before(*tempEarliestTime) {
				tempEarliestTime = &itemTime
			}

			if tempLatestTime == nil || itemTime.After(*tempLatestTime) {
				tempLatestTime = &itemTime
			}

			s.baseClient.Set(item)
			s.sortedClient.IngestItem(item, keyParams, true)

			counterLoop++
		}

		cursor.Close(context.TODO())

		if counterLoop > 0 && tempEarliestTime != nil && tempLatestTime != nil {
			if !isMostEarliestExists && !isMostRecentExists {
				// Cache is empty, set initial boundaries
				mostEarliestTimeOnCache = tempEarliestTime
				mostRecentTimeOnCache = tempLatestTime
				isMostEarliestExists = true
				isMostRecentExists = true
				needsEarliestUpdate = true
				needsRecentUpdate = true
			} else {
				// Update boundaries if they extend beyond current cache
				if !isMostEarliestExists || (mostEarliestTimeOnCache != nil && tempEarliestTime.Before(*mostEarliestTimeOnCache)) {
					mostEarliestTimeOnCache = tempEarliestTime
					isMostEarliestExists = true
					needsEarliestUpdate = true
				}

				if !isMostRecentExists || (mostRecentTimeOnCache != nil && tempLatestTime.After(*mostRecentTimeOnCache)) {
					mostRecentTimeOnCache = tempLatestTime
					isMostRecentExists = true
					needsRecentUpdate = true
				}
			}
		}
	}

	// Update Redis only if boundaries changed
	if needsEarliestUpdate && mostEarliestTimeOnCache != nil {
		if err := s.sortedClient.SetMostEarliestItem(keyParams, *mostEarliestTimeOnCache); err != nil {
			return err
		}
	}

	if needsRecentUpdate && mostRecentTimeOnCache != nil {
		if err := s.sortedClient.SetMostRecentItem(keyParams, *mostRecentTimeOnCache); err != nil {
			return err
		}
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
