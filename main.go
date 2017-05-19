package main

import (
	"github.com/neverlee/glog"
	"net/http"
	"os"
	"sync/backend"
	"syscall"
	"time"
)

type APIHandler func(*Request)

func RegisterHandleFunc(path string, handlerFunc APIHandler) {
	http.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(newRequest(w, r))
	})
}

func listenAndServe(addr string, handler http.Handler) error {
	server := &http.Server{
		Addr:           addr,
		Handler:        handler,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxHeaderBytes: 512 * 1024,
	}
	return server.ListenAndServe()
}

func main() {
	cfgpath := "sync.conf"
	if len(os.Args) >= 2 {
		cfgpath = os.Args[1]
	}
	glog.Info("config path: ", cfgpath)
	cfg, err := backend.LoadConfig(cfgpath)
	if err != nil {
		glog.Errorln("Failed to load config: ", err)
		return
	}

	var rLimit syscall.Rlimit
	//rLimit.Max = uint64(cfg.Rlimit)
	//rLimit.Cur = uint64(cfg.Rlimit)
	rLimit.Max = uint64(8192)
	rLimit.Cur = uint64(8192)
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		glog.Error("Error Setting Rlimit: ", err)
	}

	backend.DBM, err = backend.Init_DBManager(cfg)
	if err != nil {
		glog.Errorln("Failed to Open db: ", err)
		return
	}

	glog.Info("Http Server starts Running and Serving on ", cfg.SyncHost)
	if err := listenAndServe(cfg.SyncHost, nil); err != nil {
		glog.Errorln("Http Listen and serve fail:", err)
		return
	}
}
