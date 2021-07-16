package main

import (
	"github.com/a-h/stream/example/api"
	"github.com/a-h/stream/example/api/machine"
	"github.com/akrylysov/algnhsa"

	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic("failed to create logger: " + err.Error())
	}
	h := machine.NewHandler(log, api.Store)
	algnhsa.ListenAndServe(h, nil)
}
