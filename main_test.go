package commonredis

import (
	"fmt"
	"github.com/go-redis/redismock/v9"
	"testing"
)

type Entity struct {
	*Item
	Name string
}

func TestInit(t *testing.T) {

	entity := Entity{}
	InitItem(&entity)

	fmt.Println(entity.GetUUID())
}

func TestKeySetSortedSet(t *testing.T) {

	entity := Entity{}
	InitItem(&entity)

	redis, _ := redismock.NewClientMock()

	commonRedis := Init[Entity](redis, "entity:", "entity")
	commonRedis.SetSortedSetCreatedAt(nil, entity)
}
