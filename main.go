package pageflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lefalya/item"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

type MongoItemBlueprint interface {
	item.Blueprint
	SetObjectID()
	GetObjectID() primitive.ObjectID
	GetSelf() *MongoItem
}

type MongoItem struct {
	*item.Foundation `json:",inline" bson:",inline"`
	ObjectID         primitive.ObjectID `json:"-" bson:"_id"` // MongoDB support
}

func (mi *MongoItem) SetObjectID() {
	mi.ObjectID = primitive.NewObjectID()
}

func (mi *MongoItem) GetObjectID() primitive.ObjectID {
	return mi.ObjectID
}

func (mi *MongoItem) GetSelf() *MongoItem {
	return mi
}

func InitMongoItem[T MongoItemBlueprint](mongoItem T) {
	value := reflect.ValueOf(mongoItem).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)

		// Check if the field is a pointer and is nil
		if field.Kind() == reflect.Ptr && field.IsNil() {
			// Allocate a new value for the pointer and set it
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	item.InitItem(mongoItem.GetSelf())
	mongoItem.SetObjectID()
}

type SQLItemBlueprint interface {
	item.Blueprint
	GetSelf() *SQLItem
}

type SQLItem struct {
	*item.Foundation `json:",inline" bson:",inline"`
}

func (si *SQLItem) GetSelf() *SQLItem { return si }

func InitSQLItem[T SQLItemBlueprint](sqlItem T) {
	value := reflect.ValueOf(sqlItem).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)

		// Check if the field is a pointer and is nil
		if field.Kind() == reflect.Ptr && field.IsNil() {
			// Allocate a new value for the pointer and set it
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	item.InitItem(sqlItem.GetSelf())
}

type Base[T item.Blueprint] struct {
	client        redis.UniversalClient
	itemKeyFormat string
}

func (cr *Base[T]) Get(param string) (T, error) {
	var nilItem T
	key := fmt.Sprintf(cr.itemKeyFormat, param)

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

	setExpire := cr.client.Expire(context.TODO(), key, INDIVIDUAL_KEY_TTL)
	if setExpire.Err() != nil {
		return nilItem, setExpire.Err()
	}

	return item, nil
}

func (cr *Base[T]) Set(item T, param ...string) error {
	if len(param) > 0 {
		return errors.New("only accept one param")
	}

	var key string
	if param != nil {
		key = fmt.Sprintf(cr.itemKeyFormat, param[0])
	} else {
		key = fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())
	}

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

func (cr *Base[T]) Del(item T) error {
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

func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string) *Base[T] {
	return &Base[T]{
		client:        client,
		itemKeyFormat: itemKeyFormat,
	}
}

type SortedSet[T item.Blueprint] struct {
	client             redis.UniversalClient
	sortedSetKeyFormat string
}

func (cr *SortedSet[T]) SetSortedSet(param []string, score float64, item T) error {
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

func (cr *SortedSet[T]) DeleteFromSortedSet(param []string, item T) error {
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

func (cr *SortedSet[T]) TotalItemOnSortedSet(param []string) int64 {
	key := joinParam(cr.sortedSetKeyFormat, param)

	getTotalItemSortedSet := cr.client.ZCard(context.TODO(), key)
	if getTotalItemSortedSet.Err() != nil {
		return 0
	}

	return getTotalItemSortedSet.Val()
}

func (cr *SortedSet[T]) DeleteSortedSet(param []string) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeSortedSet := cr.client.Del(context.TODO(), key)
	if removeSortedSet.Err() != nil {
		return removeSortedSet.Err()
	}

	return nil
}

func (cr *SortedSet[T]) LowestScore(param []string) (float64, error) {
	key := joinParam(cr.sortedSetKeyFormat, param)

	result, err := cr.client.ZRangeWithScores(context.TODO(), key, 0, 0).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get lowest score: %w", err)
	}

	if len(result) == 0 {
		return 0, fmt.Errorf("sorted set is empty")
	}

	return result[0].Score, nil
}

func (cr *SortedSet[T]) HighestScore(param []string) (float64, error) {
	key := joinParam(cr.sortedSetKeyFormat, param)

	result, err := cr.client.ZRangeWithScores(context.TODO(), key, -1, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get highest score: %w", err)
	}

	if len(result) == 0 {
		return 0, fmt.Errorf("sorted set is empty")
	}

	return result[0].Score, nil
}

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string) *SortedSet[T] {
	return &SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: sortedSetKeyFormat,
	}
}

type Paginate[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	itemPerPage      int64
	direction        string
	sortingReference string
}

func (cr *Paginate[T]) GetItemPerPage() int64 {
	return cr.itemPerPage
}

func (cr *Paginate[T]) GetDirection() string {
	return cr.direction
}

func (cr *Paginate[T]) AddItem(item T, sortedSetParam []string) error {
	return cr.IngestItem(item, sortedSetParam, false)
}

func (cr *Paginate[T]) IngestItem(item T, sortedSetParam []string, seed bool) error {
	if cr.direction == "" {
		return errors.New("must set direction!")
	}

	score, err := getItemScore(item, cr.sortingReference)
	if err != nil {
		return err
	}

	isFirstPage, err := cr.IsFirstPage(sortedSetParam)
	if err != nil {
		return err
	}

	isLastPage, err := cr.IsLastPage(sortedSetParam)
	if err != nil {
		return err
	}

	currentItemScore := float64(item.GetCreatedAt().UnixMilli())

	if !seed {
		isBlankPage, errGet := cr.IsBlankPage(sortedSetParam)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			cr.DelBlankPage(sortedSetParam)
		}

		if cr.direction == Descending {
			if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
				lowestScore, err := cr.sortedSetClient.LowestScore(sortedSetParam)
				if err != nil {
					return err
				}

				if currentItemScore >= lowestScore {
					if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(sortedSetParam)
					}
					return cr.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
				}
			}
		} else if cr.direction == Ascending {
			if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
				highestScore, err := cr.sortedSetClient.HighestScore(sortedSetParam)
				if err != nil {
					return err
				}

				if currentItemScore <= highestScore {
					if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) == cr.itemPerPage && isFirstPage {
						return cr.DelFirstPage(sortedSetParam)
					}
					if isFirstPage || isLastPage {
						return cr.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
					}
				}
			}
		}
	} else {
		return cr.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
	}

	return nil
}

func (cr *Paginate[T]) RemoveItem(item T, param []string) error {
	err := cr.sortedSetClient.DeleteFromSortedSet(param, item)
	if err != nil {
		return err
	}

	isFirstPage, errFirstPage := cr.IsFirstPage(param)
	if errFirstPage != nil {
		return errFirstPage
	}
	if isFirstPage {
		numItem := cr.sortedSetClient.TotalItemOnSortedSet(param) // O(log(n))
		if numItem == 0 {
			errRemFirstPage := cr.DelFirstPage(param)
			if errRemFirstPage != nil {
				return errRemFirstPage
			}
		}
	}

	isLastPage, errLastPage := cr.IsLastPage(param)
	if errLastPage != nil {
		return errLastPage
	}
	if isLastPage {
		numItem := cr.sortedSetClient.TotalItemOnSortedSet(param)
		if numItem == 0 {
			errRemLastPage := cr.DelLastPage(param)
			if errRemLastPage != nil {
				return errRemLastPage
			}
		}
	}

	return nil
}

func (cr *Paginate[T]) IsFirstPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
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
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
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
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	setFirstPageKey := cr.client.Del(context.TODO(), firstPageKey)
	if setFirstPageKey.Err() != nil {
		return setFirstPageKey.Err()
	}

	return nil
}

func (cr *Paginate[T]) IsLastPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
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
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
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
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	delLastPageKey := cr.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (cr *Paginate[T]) IsBlankPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

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

func (cr *Paginate[T]) SetBlankPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

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

func (cr *Paginate[T]) DelBlankPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	delLastPageKey := cr.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (cr *Paginate[T]) Fetch(
	param []string,
	lastRandIds []string,
	processorArgs []interface{},
	processor func(item *T, args []interface{}),
) ([]T, string, string, error) {
	var items []T
	var validLastRandId string
	var position string

	// safety net
	if cr.direction == "" {
		return nil, validLastRandId, position, errors.New("must set direction!")
	}

	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	start := int64(0)
	stop := cr.itemPerPage - 1

	for i := len(lastRandIds) - 1; i >= 0; i-- {
		item, err := cr.baseClient.Get(lastRandIds[i])
		if err != nil {
			continue
		}

		var rank *redis.IntCmd
		if cr.direction == Descending {
			rank = cr.client.ZRevRank(context.TODO(), sortedSetKey, item.GetRandId())
		} else {
			rank = cr.client.ZRank(context.TODO(), sortedSetKey, item.GetRandId())
		}

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
		item, err := cr.baseClient.Get(listRandIds[i])
		if err != nil {
			continue
		}
		if processor != nil {
			processor(&item, processorArgs)
		}
		items = append(items, item)
		validLastRandId = listRandIds[i]
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

func (cr *Paginate[T]) FetchAll(param []string) ([]T, error) {
	return FetchAll(cr.client, cr.baseClient, cr.sortedSetClient, param, cr.direction)
}

func (cr *Paginate[T]) RequriesSeeding(param []string, totalItems int64) (bool, error) {
	isBlankPage, err := cr.IsBlankPage(param)
	if err != nil {
		return false, err
	}

	isFirstPage, err := cr.IsFirstPage(param)
	if err != nil {
		return false, err
	}

	isLastPage, err := cr.IsLastPage(param)
	if err != nil {
		return false, err
	}

	if !isBlankPage && !isFirstPage && !isLastPage && totalItems < cr.itemPerPage {
		return true, nil
	} else {
		return false, nil
	}
}

func (cr *Paginate[T]) RemovePagination(param []string) error {
	err := cr.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	err = cr.DelFirstPage(param)
	if err != nil {
		return err
	}

	err = cr.DelLastPage(param)
	if err != nil {
		return err
	}

	err = cr.DelBlankPage(param)
	if err != nil {
		return err
	}

	return nil
}

func (cr *Paginate[T]) PurgePagination(param []string) error {
	items, err := cr.FetchAll(param)
	if err != nil {
		return err
	}

	for _, item := range items {
		cr.baseClient.Del(item)
	}

	err = cr.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	err = cr.DelFirstPage(param)
	if err != nil {
		return err
	}

	err = cr.DelLastPage(param)
	if err != nil {
		return err
	}

	err = cr.DelBlankPage(param)
	if err != nil {
		return err
	}

	return nil
}

func NewPaginateWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, sortingReference string) *Paginate[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
	}

	return &Paginate[T]{
		client:           client,
		baseClient:       baseClient,
		sortedSetClient:  &sortedSetClient,
		itemPerPage:      itemPerPage,
		direction:        direction,
		sortingReference: sortingReference,
	}
}

func NewPaginate[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string) *Paginate[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
	}

	return &Paginate[T]{
		client:          client,
		baseClient:      baseClient,
		sortedSetClient: &sortedSetClient,
		itemPerPage:     itemPerPage,
		direction:       direction,
	}
}

type Segment struct {
	*item.Foundation `json:",inline" bson:",inline"`
	Start            float64
	End              float64
}

func (segment *Segment) SetStart(start float64) {
	segment.Start = start
}

func (segment *Segment) SetEnd(end float64) {
	segment.End = end
}

func NewSegment(start float64, end float64) Segment {
	segment := &Segment{}
	item.InitItem(segment)
	return *segment
}

type SegmentManager[T item.Blueprint] struct {
	client             redis.UniversalClient
	baseClient         *Base[Segment]
	sortedSetClient    *SortedSet[Segment]
	segmentDesignation string
}

func (sm *SegmentManager[T]) AddSegment(start float64, end float64) {
	segment := NewSegment(start, end)
	segment.SetStart(start)
	segment.SetEnd(end)

	totalItemOnSortedSet := sm.sortedSetClient.TotalItemOnSortedSet([]string{sm.segmentDesignation})

	sm.baseClient.Set(segment)
	sm.sortedSetClient.SetSortedSet([]string{sm.segmentDesignation}, float64(totalItemOnSortedSet+1), segment)
}

func (sm *SegmentManager[T]) IsWithinSegment(start float64, end float64) *Segment {
	keySegmentList := joinParam(sm.sortedSetClient.sortedSetKeyFormat, []string{sm.segmentDesignation})

	segments, errFetchSegments := sm.client.ZRange(context.TODO(), keySegmentList, 0, -1).Result()
	if errFetchSegments != nil {
		return nil
	}

	for _, segmentRandId := range segments {
		segment, err := sm.baseClient.Get(segmentRandId)
		if err != nil {
			continue
		}

		if segment.Start <= start && segment.End >= end {
			return &segment
		}
	}
	return nil
}

func NewSegmentManager[T item.Blueprint](client redis.UniversalClient, designation string) *SegmentManager[T] {
	baseClient := &Base[Segment]{
		client:        client,
		itemKeyFormat: "segment:%s",
	}

	sortedSetClient := &SortedSet[Segment]{
		client:             client,
		sortedSetKeyFormat: "segments:%s",
	}

	return &SegmentManager[T]{
		segmentDesignation: designation,
		client:             client,
		baseClient:         baseClient,
		sortedSetClient:    sortedSetClient,
	}
}

type Sorted[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	direction        string
	sortingReference string
}

func (srtd *Sorted[T]) SetDirection(direction string) {
	if direction != Ascending && direction != Descending {
		direction = Descending
	} else {
		srtd.direction = direction
	}
}

func (srtd *Sorted[T]) AddItem(item T, sortedSetParam []string) {
	srtd.IngestItem(item, sortedSetParam, false)
}

func (srtd *Sorted[T]) IngestItem(item T, sortedSetParam []string, seed bool) error {
	score, err := getItemScore(item, srtd.sortingReference)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := srtd.IsBlankPage(sortedSetParam)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			srtd.DelBlankPage(sortedSetParam)
		}

		if srtd.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
			return srtd.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
		}
	} else {
		return srtd.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
	}

	return nil
}

func (srtd *Sorted[T]) RemoveItem(item T, sortedSetParam []string) error {
	return srtd.sortedSetClient.DeleteFromSortedSet(sortedSetParam, item)
}

func (srtd *Sorted[T]) Fetch(param []string) ([]T, error) {
	return FetchAll[T](srtd.client, srtd.baseClient, srtd.sortedSetClient, param, srtd.direction)
}

func (srtd *Sorted[T]) SetBlankPage(param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	setLastPageKey := srtd.client.Set(
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

func (srtd *Sorted[T]) DelBlankPage(param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	delLastPageKey := srtd.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (srtd *Sorted[T]) IsBlankPage(param []string) (bool, error) {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	getLastPageKey := srtd.client.Get(context.TODO(), lastPageKey)
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

func (srtd *Sorted[T]) RequireSeeding(param []string) (bool, error) {
	isBlankPage, err := srtd.IsBlankPage(param)
	if err != nil {
		return false, err
	}

	if !isBlankPage {
		if srtd.sortedSetClient.TotalItemOnSortedSet(param) > 0 {
			return false, nil
		}
		return true, nil
	} else {
		return false, nil
	}
}

func (srtd *Sorted[T]) RemoveSorted(param []string) error {
	err := srtd.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	return nil
}

func (srtd *Sorted[T]) PurgeSorted(param []string) error {
	items, err := srtd.Fetch(param)
	if err != nil {
		return err
	}

	for _, item := range items {
		srtd.baseClient.Del(item)
	}

	err = srtd.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	return nil
}

func NewSortedWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, direction string, sortingReference string) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
	}

	return &Sorted[T]{
		client:           client,
		baseClient:       baseClient,
		sortedSetClient:  sortedSetClient,
		direction:        direction,
		sortingReference: sortingReference,
	}
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, direction string) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
	}

	return &Sorted[T]{
		client:          client,
		baseClient:      baseClient,
		sortedSetClient: sortedSetClient,
		direction:       direction,
	}
}

func FetchAll[T item.Blueprint](redisClient redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], param []string, direction string) ([]T, error) {
	var items []T
	var extendTTL bool

	if direction == "" {
		return nil, errors.New("must set direction!")
	}

	sortedSetKey := joinParam(sortedSetClient.sortedSetKeyFormat, param)

	var result *redis.StringSliceCmd
	if direction == Descending {
		result = redisClient.ZRevRange(context.TODO(), sortedSetKey, 0, -1)
	} else {
		result = redisClient.ZRange(context.TODO(), sortedSetKey, 0, -1)
	}

	if result.Err() != nil {
		return nil, result.Err()
	}
	listRandIds := result.Val()

	for i := 0; i < len(listRandIds); i++ {
		if !extendTTL {
			extendTTL = true
		}

		item, err := baseClient.Get(listRandIds[i])
		if err != nil {
			continue
		}
		items = append(items, item)
	}

	if extendTTL {
		redisClient.Expire(context.TODO(), sortedSetKey, SORTED_SET_TTL)
	}

	return items, nil
}

func getItemScore[T item.Blueprint](item T, sortingReference string) (float64, error) {
	if sortingReference == "" || sortingReference == "createdAt" {
		if scorer, ok := interface{}(item).(interface{ GetCreatedAt() time.Time }); ok {
			return float64(scorer.GetCreatedAt().UnixMilli()), nil
		}
	}

	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return 0, errors.New("getItemScore: item must be a struct or pointer to struct")
	}

	field := val.FieldByName(sortingReference)
	if !field.IsValid() {
		return 0, fmt.Errorf("getItemScore: field %s not found in item", sortingReference)
	}

	switch field.Type() {
	case reflect.TypeOf(time.Time{}):
		return float64(field.Interface().(time.Time).UnixMilli()), nil
	case reflect.TypeOf(&time.Time{}):
		if field.IsNil() {
			return 0, errors.New("getItemScore: time field is nil")
		}
		return float64(field.Interface().(*time.Time).UnixMilli()), nil
	case reflect.TypeOf(int64(0)):
		return float64(field.Interface().(int64)), nil
	default:
		return 0, fmt.Errorf("getItemScore: field %s is not a time.Time", sortingReference)
	}
}
