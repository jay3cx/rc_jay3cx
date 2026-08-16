package main

import (
	"io"
	"log"
	"net/http"
	"sync/atomic"
)

func main() {
	var flakyCalls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusNoContent)
		case "/flaky":
			if flakyCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/bad":
			w.WriteHeader(http.StatusBadRequest)
		default:
			http.NotFound(w, r)
		}
	})
	log.Print("mock vendor listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", handler))
}
