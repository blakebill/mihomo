package route

import (
	"strconv"

	"github.com/metacubex/mihomo/common/dialfeedback"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func dartRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/dial-feedback", getDialFeedback)
	return r
}

func getDialFeedback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	var since uint64
	if value := r.URL.Query().Get("since"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}
		since = parsed
	}
	if r.URL.Query().Get("signals") == "1" {
		render.JSON(w, r, dialfeedback.Default.SnapshotDetailedSince(since))
		return
	}
	render.JSON(w, r, dialfeedback.Default.SnapshotSince(since))
}
