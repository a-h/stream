package main

import (
	"github.com/a-h/stream/example/api"
	"github.com/a-h/stream/example/api/machine/insertcoin"
	"github.com/akrylysov/algnhsa"

	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic("failed to create logger: " + err.Error())
	}
	h := insertcoin.NewHandler(log, api.Store)
	algnhsa.ListenAndServe(h, nil)
}
