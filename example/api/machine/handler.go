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
	switch r.Method {
	case http.MethodPost:
		h.Post(w, r)
		return
	case http.MethodGet:
		h.Get(w, r)
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
	p, err := stream.New(h.Store, id, machine)
	if err != nil {
		h.Log.Error("failed to create new stream processor", zap.Error(err))
		http.Error(w, "failed to create new stream processor", http.StatusInternalServerError)
		return
	}
	err = p.Process()
	if err != nil {
		h.Log.Error("failed to create new machine", zap.Error(err))
		http.Error(w, "failed to create new machine", http.StatusInternalServerError)
		return
	}

	enc := json.NewEncoder(w)
	err = enc.Encode(machine)
	if err != nil {
		h.Log.Error("failed to encode machine", zap.String("id", id), zap.Error(err))
		http.Error(w, "failed to encode machine", http.StatusInternalServerError)
		return
	}
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

	machine := models.NewSlotMachine(id)
	_, err := h.Store.Get(id, machine)
	if err != nil {
		h.Log.Error("failed to get machine", zap.Error(err))
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
