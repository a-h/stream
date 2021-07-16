package main

import (
	"net/http"

	"github.com/akrylysov/algnhsa"
)

func main() {
	notFoundHandler := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}
	algnhsa.ListenAndServe(http.HandlerFunc(notFoundHandler), nil)
}
