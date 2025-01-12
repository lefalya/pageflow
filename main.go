package commonredis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/lefalya/commonredis/interfaces"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"reflect"
	"time"
)

const (
	FORMATTED_TIME     = "2006-01-02T15:04:05.000000000Z"
	DAY                = 24 * time.Hour
	INDIVIDUAL_KEY_TTL = DAY * 7
	SORTED_SET_TTL     = DAY * 2
	RANDID_LENGTH      = 16
)

var (
	KEY_NOT_FOUND      = errors.New("(commonredis) key not found!")
	REDIS_FATAL_ERROR  = errors.New("(commonredis) Redis fatal error!")
	ERROR_PARSE_JSON   = errors.New("(commonredis) Parse json fatal error!")
	ERROR_MARSHAL_JSON = errors.New("(commoncrud) error marshal json!")
)

func RandId() string {
	// Define the characters that can be used in the random string
	characters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Initialize an empty string to store the result
	result := make([]byte, RANDID_LENGTH)

	// Generate random characters for the string
	for i := 0; i < RANDID_LENGTH; i++ {
		result[i] = characters[rand.Intn(len(characters))]
	}

	return string(result)
}

func joinParam(keyFormat string, param []string) string {
	interfaces := make([]interface{}, len(param))
	for i, v := range param {
		interfaces[i] = v
	}
	sortedSetKey := fmt.Sprintf(keyFormat, interfaces...)
	return sortedSetKey
}

type Item struct {
	UUID            string    `bson:"uuid"`
	RandId          string    `bson:"randid"`
	CreatedAt       time.Time `json:"-" bson:"-"`
	UpdatedAt       time.Time `json:"-" bson:"-"`
	CreatedAtString string    `bson:"createdat"`
	UpdatedAtString string    `bson:"updatedat"`
}

func (i *Item) SetUUID() {
	i.UUID = uuid.New().String()
}

func (i *Item) GetUUID() string {
	return i.UUID
}

func (i *Item) SetRandId() {
	i.RandId = RandId()
}

func (i *Item) GetRandId() string {
	return i.RandId
}

func (i *Item) SetCreatedAt(time time.Time) {
	i.CreatedAt = time
}

func (i *Item) SetUpdatedAt(time time.Time) {
	i.UpdatedAt = time
}

func (i *Item) GetCreatedAt() time.Time {
	return i.CreatedAt
}

func (i *Item) GetUpdatedAt() time.Time {
	return i.UpdatedAt
}

func (i *Item) SetCreatedAtString(timeString string) {
	i.CreatedAtString = timeString
}

func (i *Item) SetUpdatedAtString(timeString string) {
	i.UpdatedAtString = timeString
}

func (i *Item) GetCreatedAtString() string {
	return i.CreatedAtString
}

func (i *Item) GetUpdatedAtString() string {
	return i.UpdatedAtString
}

func InitItem[T interfaces.Item](item T) {
	currentTime := time.Now().In(time.UTC)
	value := reflect.ValueOf(item).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)

		// Check if the field is a pointer and is nil
		if field.Kind() == reflect.Ptr && field.IsNil() {
			// Allocate a new value for the pointer and set it
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	item.SetUUID()
	item.SetRandId()
	item.SetCreatedAt(currentTime)
	item.SetUpdatedAt(currentTime)
	item.SetCreatedAtString(currentTime.Format(FORMATTED_TIME))
	item.SetUpdatedAtString(currentTime.Format(FORMATTED_TIME))
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
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
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
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
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
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
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

func (cr *CommonRedis[T]) SetSortedSetCreatedAt(param []string, item T) *Error {
	cr.DelSettled(param)
	var key string
	if param == nil {
		key = cr.sortedSetKeyFormat
	} else {
		key = joinParam(cr.sortedSetKeyFormat, param)
	}

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
	key := joinParam(cr.sortedSetKeyFormat, param)

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
	key := joinParam(cr.sortedSetKeyFormat, param)

	getTotalItemSortedSet := cr.client.ZCard(context.TODO(), key)
	if getTotalItemSortedSet.Err() != nil {
		return 0
	}

	return getTotalItemSortedSet.Val()
}

func (cr *CommonRedis[T]) DeleteSortedSet(param []string) *Error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeSortedSet := cr.client.Del(context.TODO(), key)
	if removeSortedSet.Err() != nil {
		return &Error{
			Err:     REDIS_FATAL_ERROR,
			Details: removeSortedSet.Err().Error(),
		}
	}

	return nil
}
