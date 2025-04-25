package mongo

import (
	"context"
	"errors"
	"github.com/lefalya/pageflow"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	NoDatabaseProvided           = errors.New("No database provided!")
	DocumentOrReferencesNotFound = errors.New("Document or References not found!")
)

type MongoSeeder[T pageflow.MongoItemBlueprint] struct {
	coll             *mongo.Collection
	baseClient       *pageflow.Base[T]
	paginationClient *pageflow.Paginate[T]
}

func (m *MongoSeeder[T]) FindOne(key string, value string, initItem func() T) (T, error) {
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

func (m *MongoSeeder[T]) SeedOne(key string, value string, initItem func() T) error {
	item, err := m.FindOne(key, value, initItem)
	if err != nil {
		return err
	}

	err = m.baseClient.Set(item)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoSeeder[T]) SeedPartial(subtraction int64, validLastRandId string, query bson.D, paginateParams []string, initItem func() T) error {
	var cursor *mongo.Cursor
	var reference T
	var withReference bool
	var err error
	var firstPage bool
	var filter bson.D
	var errorDecode error
	var compOp string

	if query == nil {
		query = bson.D{}
	}

	findOptions := options.Find()
	if m.paginationClient.GetDirection() == pageflow.Ascending {
		findOptions.SetSort(bson.D{{"_id", 1}})
		compOp = "$gt"
	} else {
		findOptions.SetSort(bson.D{{"_id", -1}})
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
		filter = bson.D{
			{"$and",
				bson.A{
					query,
					bson.D{
						{"_id", bson.D{{compOp, reference.GetObjectID()}}},
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

func (m *MongoSeeder[T]) SeedAll(query bson.D, listKey string, initItem func() T) error {
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
		m.paginationClient.IngestItem(item, []string{listKey}, true)
	}

	return nil
}

func NewMongoSeeder[T pageflow.MongoItemBlueprint](coll *mongo.Collection, baseClient *pageflow.Base[T], paginateClient *pageflow.Paginate[T]) *MongoSeeder[T] {
	return &MongoSeeder[T]{
		coll:             coll,
		baseClient:       baseClient,
		paginationClient: paginateClient,
	}
}
