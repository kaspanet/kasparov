package server

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/kaspanet/kasparov/httpserverutils"
)

const gracefulShutdownTimeout = 30 * time.Second

// Start starts the HTTP REST server and returns a
// function to gracefully shutdown it.
func Start(listenAddr string) func() {
	router := mux.NewRouter()
	router.Use(httpserverutils.AddRequestMetadataMiddleware)
	router.Use(httpserverutils.RecoveryMiddleware)
	router.Use(httpserverutils.LoggingMiddleware)
	router.Use(httpserverutils.SetJSONMiddleware)
	addRoutes(router)
	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: handlers.CORS()(router),
	}
	spawn("server-Start", func() {
		log.Infof("Kasparovd is listening on %s", listenAddr)
		log.Errorf("%s", httpServer.ListenAndServe())
	})

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()
		err := httpServer.Shutdown(ctx)
		if err != nil {
			log.Errorf("Error shutting down HTTP server: %s", err)
		}
	}
}
