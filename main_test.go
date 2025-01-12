package commonredis

import (
	"fmt"
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
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

	commonRedis := Init[Entity](redis, "entity:%s", "entity")
	commonRedis.SetSortedSetCreatedAt(nil, entity)
}

func TestJoinParam(t *testing.T) {
	formatKey := "id:%s:name:%s:contanct:%s"
	param := []string{"jd3ruj39f", "nino", "yustika"}

	joinResult := joinParam(formatKey, param)
	assert.NotNil(t, joinResult)
}
