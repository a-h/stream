package api

import (
	"os"

	"github.com/a-h/stream"
)

var TableName = os.Getenv("MACHINE_TABLE")
var Store stream.Store

func init() {
	var err error
	Store, err = stream.NewStore(TableName, "machine")
	if err != nil {
		panic("failed to init store: " + err.Error())
	}
}
