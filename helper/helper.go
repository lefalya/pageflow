package helper

import (
	"errors"
	"fmt"
	"github.com/lefalya/item"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"time"
)

func GetItemScore[T item.Blueprint](item T, sortingReference string) (interface{}, error) {
	if sortingReference == "" || sortingReference == "createdat" || sortingReference == "createdAt" {
		if scorer, ok := interface{}(item).(interface{ GetCreatedAt() time.Time }); ok {
			return scorer.GetCreatedAt(), nil
		}
	}

	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, errors.New("getItemScore: item must be a struct or pointer to struct")
	}

	field := val.FieldByName(sortingReference)
	if !field.IsValid() {
		return nil, fmt.Errorf("getItemScore: field %s not found in item", sortingReference)
	}

	switch field.Type() {
	case reflect.TypeOf(time.Time{}):
		return field.Interface().(time.Time), nil
	case reflect.TypeOf(&time.Time{}):
		if field.IsNil() {
			return nil, errors.New("getItemScore: time field is nil")
		}
		return field.Interface().(*time.Time), nil
	case reflect.TypeOf(int64(0)):
		return field.Interface().(int64), nil
	case reflect.TypeOf(float64(0)):
		return field.Interface().(float64), nil
	case reflect.TypeOf(int(0)):
		return int64(field.Interface().(int)), nil
	case reflect.TypeOf(int32(0)):
		return int64(field.Interface().(int32)), nil
	default:
		return nil, fmt.Errorf("getItemScore: field %s is not a supported type (time.Time, *time.Time, int64, float64, int, int32)", sortingReference)
	}
}

// Helper function to convert the result to float64 if needed for scoring
func GetItemScoreAsFloat64[T item.Blueprint](item T, sortingReference string) (float64, error) {
	result, err := GetItemScore(item, sortingReference)
	if err != nil {
		return 0, err
	}

	switch v := result.(type) {
	case time.Time:
		return float64(v.UnixMilli()), nil
	case *time.Time:
		return float64(v.UnixMilli()), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("getItemScoreAsFloat64: unsupported type %T", v)
	}
}

// Helper function to get the result as time.Time if possible
func GetItemScoreAsTime[T item.Blueprint](item T, sortingReference string) (time.Time, error) {
	result, err := GetItemScore(item, sortingReference)
	if err != nil {
		return time.Time{}, err
	}

	switch v := result.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		return *v, nil
	case int64:
		return time.UnixMilli(v), nil
	case float64:
		return time.UnixMilli(int64(v)), nil
	case primitive.ObjectID:
		return v.Timestamp(), nil
	case string:
		// Handle ObjectId as string
		if objID, err := primitive.ObjectIDFromHex(v); err == nil {
			return objID.Timestamp(), nil
		}
		// Try parsing as time string
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("getItemScoreAsTime: cannot parse string %s as time", v)
	default:
		return time.Time{}, fmt.Errorf("getItemScoreAsTime: cannot convert %T to time.Time", v)
	}
}
