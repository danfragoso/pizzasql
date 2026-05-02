//go:build pprof

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"
)

var (
	pprofAddr          = flag.String("pprof", "", "Enable pprof debug server on this address (e.g. localhost:6060)")
	pprofBlockRate     = flag.Int("pprof-block-rate", 0, "Set runtime block profile rate when pprof is enabled (0 disables block profiling)")
	pprofMutexFraction = flag.Int("pprof-mutex-fraction", 0, "Set runtime mutex profile fraction when pprof is enabled (0 disables mutex profiling)")
)

func init() {
	startPprofServerHook = startPprofServer
}

func startPprofServer() *http.Server {
	if *pprofAddr == "" {
		return nil
	}

	if *pprofBlockRate > 0 {
		runtime.SetBlockProfileRate(*pprofBlockRate)
	}
	if *pprofMutexFraction > 0 {
		runtime.SetMutexProfileFraction(*pprofMutexFraction)
	}

	srv := &http.Server{
		Addr:         *pprofAddr,
		Handler:      http.DefaultServeMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 120 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "pprof server error: %v\n", err)
		}
	}()

	return srv
}
