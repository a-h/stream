package insertcoin

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/a-h/pathvars"
	"github.com/a-h/stream"
	"github.com/a-h/stream/example/api/models"
	"go.uber.org/zap"
)

var matcher = pathvars.NewExtractor("*/machine/{id}/insertCoin")

func NewHandler(log *zap.Logger, s stream.Store) (h Handler) {
	h.Log = log
	h.Store = s
	return
}

type Handler struct {
	Log   *zap.Logger
	Store stream.Store
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		h.Post(w, r)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func (h Handler) Post(w http.ResponseWriter, r *http.Request) {
	defer h.Log.Sync()

	pathValues, ok := matcher.Extract(r.URL)
	if !ok {
		http.Error(w, "path not found", http.StatusNotFound)
		return
	}
	id, ok := pathValues["id"]
	if !ok {
		http.Error(w, "missing id parameter in path", http.StatusNotFound)
		return
	}

	machine := models.NewSlotMachine(id)
	p, err := stream.Load(h.Store, id, machine)
	if err != nil {
		h.Log.Error("failed to load machine", zap.Error(err))
		http.Error(w, "failed to load machine", http.StatusInternalServerError)
		return
	}

	// You might need to load the model from the HTTP body, but here we're not expecting one.
	err = p.Process(models.InsertCoin{})
	if err != nil {
		if err == models.ErrCannotInsertCoin {
			http.Error(w, fmt.Sprintf("cannot insert coin, is there one already in the slot?"), http.StatusNotAcceptable)
			return
		}
		h.Log.Error("failed to insert coin", zap.Error(err))
		http.Error(w, fmt.Sprintf("internal server error"), http.StatusInternalServerError)
		return
	}

	enc := json.NewEncoder(w)
	err = enc.Encode(machine)
	if err != nil {
		h.Log.Error("failed to encode machine", zap.Error(err))
		http.Error(w, "failed to encode machine", http.StatusInternalServerError)
		return
	}
}
