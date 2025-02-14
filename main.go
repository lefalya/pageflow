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
	firstPage          = "FIRST_PAGE"
	middlePage         = "MIDDLE_PAGE"
	lastPage           = "LAST_PAGE"
	ascending          = "ascending"
	descending         = "descending"
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
	itemPerPage        int64
	settledKeyFormat   string
	direction          string
}

func (cr *CommonRedis[T]) SetItemKeyFormat(format string) {
	cr.itemKeyFormat = format
}

func (cr *CommonRedis[T]) SetSortedSetKeyFormat(format string) {
	cr.sortedSetKeyFormat = format
}

func (cr *CommonRedis[T]) SetItemPerPage(perPage int64) {
	cr.itemPerPage = perPage
}

func (cr *CommonRedis[T]) SetDirection(direction string) {
	if direction != ascending || direction != descending {
		direction = descending
	} else {
		cr.direction = direction
	}
}

func (cr *CommonRedis[T]) AddItem(item T, sortedSetParam []string) error {
	// safety net
	if cr.direction == "" {
		return errors.New("must set direction!")
	}

	if cr.direction == descending {
		if cr.TotalItemOnSortedSet(sortedSetParam) > 0 {
			return cr.SetSortedSet(sortedSetParam, float64(item.GetCreatedAt().UnixMilli()), item)
		}
	} else if cr.direction == ascending {
		settled, err := cr.GetSettled(sortedSetParam)
		if err != nil {
			return err
		}
		if settled {
			return cr.SetSortedSet(sortedSetParam, float64(item.GetUpdatedAt().UnixMilli()), item)
		}
	}

	return nil
}

func (cr *CommonRedis[T]) Get(randId string) (T, error) {
	var nilItem T
	key := fmt.Sprintf(cr.itemKeyFormat, randId)

	result := cr.client.Get(context.TODO(), key)
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return nilItem, redis.Nil
		}
		return nilItem, result.Err()
	}

	var item T
	errorUnmarshal := json.Unmarshal([]byte(result.Val()), &item)
	if errorUnmarshal != nil {
		return nilItem, errorUnmarshal
	}

	parsedTimeCreatedAt, _ := time.Parse(FORMATTED_TIME, item.GetCreatedAtString())
	parsedTimeUpdatedAt, _ := time.Parse(FORMATTED_TIME, item.GetUpdatedAtString())

	item.SetCreatedAt(parsedTimeCreatedAt)
	item.SetUpdatedAt(parsedTimeUpdatedAt)

	setExpire := cr.client.Expire(context.TODO(), key, INDIVIDUAL_KEY_TTL)
	if setExpire.Err() != nil {
		return nilItem, setExpire.Err()
	}

	return item, nil
}

func (cr *CommonRedis[T]) Set(item T) error {
	key := fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())

	createdAtAsString := item.GetCreatedAt().Format(FORMATTED_TIME)
	updatedAtAsString := item.GetUpdatedAt().Format(FORMATTED_TIME)

	item.SetCreatedAtString(createdAtAsString)
	item.SetUpdatedAtString(updatedAtAsString)

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return errorMarshalJson
	}

	valueAsString := string(itemInByte)
	setRedis := cr.client.Set(
		context.TODO(),
		key,
		valueAsString,
		INDIVIDUAL_KEY_TTL,
	)
	if setRedis.Err() != nil {
		return setRedis.Err()
	}

	return nil
}

func (cr *CommonRedis[T]) Del(item T) error {
	key := fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())

	deleteRedis := cr.client.Del(
		context.TODO(),
		key,
	)
	if deleteRedis.Err() != nil {
		return deleteRedis.Err()
	}

	return nil
}

func (cr *CommonRedis[T]) GetSettled(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	settledKey := sortedSetKey + ":settled"

	getSettledStatus := cr.client.Get(context.TODO(), settledKey)
	if getSettledStatus.Err() != nil {
		if getSettledStatus.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getSettledStatus.Err()
		}
	}

	if getSettledStatus.Val() == "1" {
		return true, nil
	}

	return false, nil
}

func (cr *CommonRedis[T]) SetSettled(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	settledKey := sortedSetKey + ":settled"

	setSettledKey := cr.client.Set(
		context.TODO(),
		settledKey,
		1,
		SORTED_SET_TTL,
	)

	if setSettledKey.Err() != nil {
		return setSettledKey.Err()
	}

	return nil
}

func (cr *CommonRedis[T]) DelSettled(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	settledKey := sortedSetKey + ":settled"

	setSettledKey := cr.client.Del(context.TODO(), settledKey)

	if setSettledKey.Err() != nil {
		return setSettledKey.Err()
	}

	return nil
}

func (cr *CommonRedis[T]) SetSortedSet(param []string, score float64, item T) error {
	var key string
	if param == nil {
		key = cr.sortedSetKeyFormat
	} else {
		key = joinParam(cr.sortedSetKeyFormat, param)
	}

	sortedSetMember := redis.Z{
		Score:  score,
		Member: item.GetRandId(),
	}

	setSortedSet := cr.client.ZAdd(
		context.TODO(),
		key,
		sortedSetMember)
	if setSortedSet.Err() != nil {
		return setSortedSet.Err()
	}

	setExpire := cr.client.Expire(
		context.TODO(),
		key,
		SORTED_SET_TTL,
	)
	if !setExpire.Val() {
		return setExpire.Err()
	}

	return nil
}

func (cr *CommonRedis[T]) DeleteFromSortedSet(param []string, item T) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeFromSortedSet := cr.client.ZRem(
		context.TODO(),
		key,
		item.GetRandId(),
	)
	if removeFromSortedSet.Err() != nil {
		return removeFromSortedSet.Err()
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

func (cr *CommonRedis[T]) DeleteSortedSet(param []string) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeSortedSet := cr.client.Del(context.TODO(), key)
	if removeSortedSet.Err() != nil {
		return removeSortedSet.Err()
	}

	return nil
}

func (cr *CommonRedis[T]) FetchLinked(
	param []string,
	lastRandIds []string,
) ([]T, string, string, error) {
	var items []T
	var validLastRandId string
	var position string

	// safety net
	if cr.direction == "" {
		return nil, validLastRandId, position, errors.New("must set direction!")
	}

	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	start := int64(0)
	stop := cr.itemPerPage

	for i := len(lastRandIds) - 1; i >= 0; i-- {
		item, err := cr.Get(lastRandIds[i])
		if err != nil {
			continue
		}

		rank := cr.client.ZRevRank(context.TODO(), sortedSetKey, item.GetRandId())
		if rank.Err() == nil {
			validLastRandId = item.GetRandId()
			start = rank.Val() + 1
			stop = start + cr.itemPerPage - 1
			break
		}
	}

	var listRandIds []string
	var result *redis.StringSliceCmd
	if cr.direction == descending {
		result = cr.client.ZRevRange(context.TODO(), sortedSetKey, start, stop)
	} else {
		result = cr.client.ZRange(context.TODO(), sortedSetKey, start, stop)
	}
	if result.Err() != nil {
		return nil, validLastRandId, position, result.Err()
	}
	listRandIds = result.Val()

	cr.client.Expire(context.TODO(), sortedSetKey, SORTED_SET_TTL)

	for i := 0; i < len(listRandIds); i++ {
		item, err := cr.Get(listRandIds[i])
		if err != nil {
			continue
		}
		items = append(items, item)
	}

	isSettled, errorGetSettled := cr.GetSettled(param)
	if errorGetSettled != nil {
		return nil, validLastRandId, position, errorGetSettled
	}

	if start == 0 {
		position = firstPage
	} else if int64(len(listRandIds)) < cr.itemPerPage && isSettled {
		position = lastPage
	} else {
		position = middlePage
	}

	return items, validLastRandId, position, nil
}

func Init[T interfaces.Item](client redis.UniversalClient) *CommonRedis[T] {
	return &CommonRedis[T]{
		client: client,
	}
}
