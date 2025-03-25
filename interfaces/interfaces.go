package interfaces

import (
	"github.com/lefalya/item"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MongoItem interface {
	item.Blueprint
	SetObjectID()
	GetObjectID() primitive.ObjectID
}
