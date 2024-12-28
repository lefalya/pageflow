package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lefalya/commonredis/interfaces"
	"github.com/redis/go-redis/v9"
	"time"
)

const (
	FORMATTED_TIME     = "2006-01-02T15:04:05.000000000Z"
	DAY                = 24 * time.Hour
	INDIVIDUAL_KEY_TTL = DAY * 7
	SORTED_SET_TTL     = DAY * 2
)

var (
	KEY_NOT_FOUND      = errors.New("(commonredis) key not found!")
	REDIS_FATAL_ERROR  = errors.New("(commonredis) Redis fatal error!")
	ERROR_PARSE_JSON   = errors.New("(commonredis) Parse json fatal error!")
	ERROR_MARSHAL_JSON = errors.New("(commoncrud) error marshal json!")
)

type Item struct {
	UUID            string    `bson:"uuid"`
	RandId          string    `bson:"randid"`
	CreatedAt       time.Time `json:"-" bson:"-"`
	UpdatedAt       time.Time `json:"-" bson:"-"`
	CreatedAtString string    `bson:"createdat"`
	UpdatedAtString string    `bson:"updatedat"`
}

type CommonRedis[T interfaces.Item] struct {
	client             redis.UniversalClient
	itemKeyFormat      string
	sortedSetKeyFormat string
	settledKeyFormat   string
}

type Error struct {
	Err     error
	Details string
	Message string
}

func Init[T interfaces.Item](
	client redis.UniversalClient,
	itemKeyFormat string,
	sortedSetKeyFormat string,
) *CommonRedis[T] {
	return &CommonRedis[T]{
		client:             client,
		itemKeyFormat:      itemKeyFormat,
		sortedSetKeyFormat: sortedSetKeyFormat,
	}
}

func (cr *CommonRedis[T]) Get(randId string) (T, *Error) {
	var nilItem T
	key := fmt.Sprintf(cr.itemKeyFormat, randId)

	result := cr.client.Get(context.TODO(), key)
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return nilItem, &Error{
				Err:     KEY_NOT_FOUND,
				Details: "key not found!",
			}
		}
		return nilItem, &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: result.Err().Error(),
		}
	}

	var item T
	errorUnmarshal := json.Unmarshal([]byte(result.Val()), &item)
	if errorUnmarshal != nil {
		return nilItem, &Error{
			Err:     ERROR_PARSE_JSON,
			Details: errorUnmarshal.Error(),
		}
	}

	parsedTimeCreatedAt, _ := time.Parse(FORMATTED_TIME, item.GetCreatedAtString())
	parsedTimeUpdatedAt, _ := time.Parse(FORMATTED_TIME, item.GetUpdatedAtString())

	item.SetCreatedAt(parsedTimeCreatedAt)
	item.SetUpdatedAt(parsedTimeUpdatedAt)

	setExpire := cr.client.Expire(context.TODO(), key, INDIVIDUAL_KEY_TTL)
	if setExpire.Err() != nil {
		return nilItem, &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setExpire.Err().Error(),
		}
	}

	return item, nil
}

func (cr *CommonRedis[T]) Set(item T) *Error {
	key := fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())

	createdAtAsString := item.GetCreatedAt().Format(FORMATTED_TIME)
	updatedAtAsString := item.GetUpdatedAt().Format(FORMATTED_TIME)

	item.SetCreatedAtString(createdAtAsString)
	item.SetUpdatedAtString(updatedAtAsString)

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return &Error{
			Err:     ERROR_MARSHAL_JSON,
			Details: errorMarshalJson.Error(),
		}
	}

	valueAsString := string(itemInByte)
	setRedis := cr.client.Set(
		context.TODO(),
		key,
		valueAsString,
		INDIVIDUAL_KEY_TTL,
	)
	if setRedis.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setRedis.Err().Error(),
		}
	}

	return nil
}

func (cr *CommonRedis[T]) Del(item T) *Error {
	key := fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())

	deleteRedis := cr.client.Del(
		context.TODO(),
		key,
	)
	if deleteRedis.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: deleteRedis.Err().Error(),
		}
	}

	return nil
}

func (cr *CommonRedis[T]) GetSettled(param []string) (bool, *Error) {
	sortedSetKey := fmt.Sprintf(cr.sortedSetKeyFormat, param)
	settledKey := sortedSetKey + ":settled"

	getSettledStatus := cr.client.Get(context.TODO(), settledKey)
	if getSettledStatus.Err() != nil {
		return false, &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: getSettledStatus.Err().Error(),
		}
	}

	if getSettledStatus.Val() == "1" {
		return true, nil
	}

	return false, nil
}

func (cr *CommonRedis[T]) SetSettled(param []string) *Error {
	sortedSetKey := fmt.Sprintf(cr.sortedSetKeyFormat, param)
	settledKey := sortedSetKey + ":settled"

	setSettledKey := cr.client.Set(
		context.TODO(),
		settledKey,
		1,
		SORTED_SET_TTL,
	)

	if setSettledKey.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setSettledKey.Err().Error(),
		}
	}

	return nil
}

func (cr *CommonRedis[T]) DelSettled(param []string) *Error {
	sortedSetKey := fmt.Sprintf(cr.sortedSetKeyFormat, param)
	settledKey := sortedSetKey + ":settled"

	setSettledKey := cr.client.Del(context.TODO(), settledKey)

	if setSettledKey.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setSettledKey.Err().Error(),
		}
	}

	return nil
}

func (cr *CommonRedis[T]) SetSortedSet(param []string, item T) *Error {
	cr.DelSettled(param)
	key := fmt.Sprintf(cr.sortedSetKeyFormat, param)

	sortedSetMember := redis.Z{
		Score:  float64(item.GetCreatedAt().UnixMilli()),
		Member: item.GetRandId(),
	}

	setSortedSet := cr.client.ZAdd(
		context.TODO(),
		key,
		sortedSetMember)
	if setSortedSet.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setSortedSet.Err().Error(),
		}
	}

	setExpire := cr.client.Expire(
		context.TODO(),
		key,
		SORTED_SET_TTL,
	)
	if !setExpire.Val() {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: setExpire.Err().Error(),
		}
	}

	return nil
}

func (cr *CommonRedis[T]) DeleteFromSortedSet(param []string, item T) *Error {
	key := fmt.Sprintf(cr.sortedSetKeyFormat, param)

	removeFromSortedSet := cr.client.ZRem(
		context.TODO(),
		key,
		item.GetRandId(),
	)
	if removeFromSortedSet.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeFromSortedSet.Err().Error(),
		}
	}

	return nil
}

func (cr *CommonRedis[T]) TotalItemOnSortedSet(param []string) int64 {
	key := fmt.Sprintf(cr.sortedSetKeyFormat, param)

	getTotalItemSortedSet := cr.client.ZCard(context.TODO(), key)
	if getTotalItemSortedSet.Err() != nil {
		return 0
	}

	return getTotalItemSortedSet.Val()
}

func (cr *CommonRedis[T]) DeleteSortedSet(param []string) *Error {
	key := fmt.Sprintf(cr.sortedSetKeyFormat, param)

	removeSortedSet := cr.client.Del(context.TODO(), key)
	if removeSortedSet.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeSortedSet.Err().Error(),
		}
	}

	return nil
}
