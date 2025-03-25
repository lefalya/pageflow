package pageflow

import (
	"fmt"
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

func TestJoinParam(t *testing.T) {
	formatKey := "id:%s:name:%s:contanct:%s"
	param := []string{"jd3ruj39f", "nino", "yustika"}

	joinResult := joinParam(formatKey, param)
	assert.NotNil(t, joinResult)
}
