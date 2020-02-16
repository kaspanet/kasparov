package profiling

import (
	"net"
	"net/http"

	_ "net/http/pprof"
)

func Start(port string) {
	spawn(func() {
		listenAddr := net.JoinHostPort("", port)
		log.Infof("Profile server listening on %s", listenAddr)
		profileRedirect := http.RedirectHandler("/debug/pprof", http.StatusSeeOther)
		http.Handle("/", profileRedirect)
		log.Errorf("%s", http.ListenAndServe(listenAddr, nil))
	})
}
