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
	Ascending          = "Ascending"
	Descending         = "Descending"
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

type Paginate[T interfaces.Item] struct {
	client             redis.UniversalClient
	itemKeyFormat      string
	sortedSetKeyFormat string
	itemPerPage        int64
	settledKeyFormat   string
	direction          string
}

func (cr *Paginate[T]) SetItemKeyFormat(format string) {
	cr.itemKeyFormat = format
}

func (cr *Paginate[T]) SetSortedSetKeyFormat(format string) {
	cr.sortedSetKeyFormat = format
}

func (cr *Paginate[T]) SetItemPerPage(perPage int64) {
	cr.itemPerPage = perPage
}

func (cr *Paginate[T]) SetDirection(direction string) {
	if direction != Ascending && direction != Descending {
		direction = Descending
	} else {
		cr.direction = direction
	}
}

func (cr *Paginate[T]) AddItem(item T, sortedSetParam []string) error {
	// safety net
	if cr.direction == "" {
		return errors.New("must set direction!")
	}

	isFirstPage, err := cr.IsFirstPage(sortedSetParam)
	if err != nil {
		return err
	}

	isLastPage, err := cr.IsLastPage(sortedSetParam)
	if err != nil {
		return err
	}

	if cr.direction == Descending {
		if cr.TotalItemOnSortedSet(sortedSetParam) > 0 {
			return cr.SetSortedSet(sortedSetParam, float64(item.GetCreatedAt().UnixMilli()), item)
			if cr.TotalItemOnSortedSet(sortedSetParam) > cr.itemPerPage && isFirstPage {
				cr.DelFirstPage(sortedSetParam)
			}
		}
	} else if cr.direction == Ascending {
		if cr.TotalItemOnSortedSet(sortedSetParam) == cr.itemPerPage && isFirstPage {
			return cr.DelFirstPage(sortedSetParam)
		}
		if isFirstPage || isLastPage {
			return cr.SetSortedSet(sortedSetParam, float64(item.GetUpdatedAt().UnixMilli()), item)
		}
	}

	return nil
}

func (cr *Paginate[T]) RemoveItem(item T, sortedSetParam []string) error {
	return cr.DeleteFromSortedSet(sortedSetParam, item)
}

func (cr *Paginate[T]) Get(randId string) (T, error) {
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

func (cr *Paginate[T]) Set(item T) error {
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

func (cr *Paginate[T]) Del(item T) error {
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

func (cr *Paginate[T]) IsFirstPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	fistPageKey := sortedSetKey + ":firstpage"

	getFirstPageKey := cr.client.Get(context.TODO(), fistPageKey)
	if getFirstPageKey.Err() != nil {
		if getFirstPageKey.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getFirstPageKey.Err()
		}
	}

	if getFirstPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Paginate[T]) SetFirstPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	setFirstPageKey := cr.client.Set(
		context.TODO(),
		firstPageKey,
		1,
		SORTED_SET_TTL,
	)

	if setFirstPageKey.Err() != nil {
		return setFirstPageKey.Err()
	}
	return nil
}

func (cr *Paginate[T]) DelFirstPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":settled"

	setFirstPageKey := cr.client.Del(context.TODO(), firstPageKey)
	if setFirstPageKey.Err() != nil {
		return setFirstPageKey.Err()
	}

	return nil
}

func (cr *Paginate[T]) IsLastPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	getLastPageKey := cr.client.Get(context.TODO(), lastPageKey)
	if getLastPageKey.Err() != nil {
		if getLastPageKey.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getLastPageKey.Err()
		}
	}

	if getLastPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Paginate[T]) SetLastPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	setLastPageKey := cr.client.Set(
		context.TODO(),
		lastPageKey,
		1,
		SORTED_SET_TTL,
	)

	if setLastPageKey.Err() != nil {
		return setLastPageKey.Err()
	}
	return nil
}

func (cr *Paginate[T]) DelLastPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	delLastPageKey := cr.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (cr *Paginate[T]) SetSortedSet(param []string, score float64, item T) error {
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

func (cr *Paginate[T]) DeleteFromSortedSet(param []string, item T) error {
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

func (cr *Paginate[T]) TotalItemOnSortedSet(param []string) int64 {
	key := joinParam(cr.sortedSetKeyFormat, param)

	getTotalItemSortedSet := cr.client.ZCard(context.TODO(), key)
	if getTotalItemSortedSet.Err() != nil {
		return 0
	}

	return getTotalItemSortedSet.Val()
}

func (cr *Paginate[T]) DeleteSortedSet(param []string) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeSortedSet := cr.client.Del(context.TODO(), key)
	if removeSortedSet.Err() != nil {
		return removeSortedSet.Err()
	}

	return nil
}

func (cr *Paginate[T]) FetchLinked(
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
	if cr.direction == Descending {
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

	if start == 0 {
		position = firstPage
	} else if int64(len(listRandIds)) < cr.itemPerPage {
		position = lastPage
	} else {
		position = middlePage
	}

	return items, validLastRandId, position, nil
}

func NewPaginate[T interfaces.Item](client redis.UniversalClient) *Paginate[T] {
	return &Paginate[T]{
		client: client,
	}
}

type EventQueue[T interfaces.Item] struct {
	client     redis.UniversalClient
	name       string
	throughput int64 // number of events per minute
	duration   time.Duration
}

func (eq *EventQueue[T]) Add(ctx context.Context, item T) error {
	err := eq.client.LPush(ctx, eq.name, item.GetRandId())
	if err != nil {
		return err.Err()
	}

	return nil
}

func (eq *EventQueue[T]) Worker(ctx context.Context, processor func(string) error, errorHandler func(error)) {
	ticker := time.NewTicker(eq.duration)
	defer ticker.Stop()

	for range ticker.C {
		randid, err := eq.client.RPop(ctx, eq.name).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			errorHandler(err)
			continue
		}

		err = processor(randid)
		if err != nil {
			errorHandler(err)
		}
	}
}

func NewEventQueue[T interfaces.Item](redis *redis.UniversalClient, name string, throughput int64) *EventQueue[T] {
	duration := time.Duration(60/throughput) * time.Second

	return &EventQueue[T]{
		client:     *redis,
		name:       name,
		throughput: throughput,
		duration:   duration,
	}
}
