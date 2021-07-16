package machine

import (
	"encoding/json"
	"net/http"

	"github.com/a-h/pathvars"
	"github.com/a-h/stream"
	"github.com/a-h/stream/example/api/models"
	"go.uber.org/zap"
)

var matcher = pathvars.NewExtractor("*/machine/{id}")

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
	if r.Method == http.MethodGet {
		h.Get(w, r)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func (h Handler) Get(w http.ResponseWriter, r *http.Request) {
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

	machine := models.NewSlotMachine()
	_, err := h.Store.Get(id, machine)
	if err != nil {
		http.Error(w, "failed to get machine", http.StatusInternalServerError)
		return
	}

	enc := json.NewEncoder(w)
	err = enc.Encode(machine)
	if err != nil {
		http.Error(w, "failed to encode machine", http.StatusInternalServerError)
		return
	}
}
