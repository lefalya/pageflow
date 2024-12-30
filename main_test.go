package commonredis

import (
	"fmt"
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
