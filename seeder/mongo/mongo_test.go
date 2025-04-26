package mongo

import (
	"fmt"
	"testing"
	"time"
)

func TestBuildQuery(t *testing.T) {
	queryWithoutTimeRange := BuildAttributeFilter("campaignUUID", "8c7970f7-b524-438d-8f2e-73f3bc985e26")
	queryWithTimeRange := CombineFilters(
		BuildAttributeFilter("accountID", "8c7970f7-b524-438d-8f2e-73f3bc985e26"),
		BuildTimeRangeFilter("createdat", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC)),
	)
	fmt.Println(queryWithoutTimeRange)
	fmt.Println(queryWithTimeRange)
}
