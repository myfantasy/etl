package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/myfantasy/mfe"
	"github.com/myfantasy/mfetl"

	"github.com/myfantasy/mfh"
	log "github.com/sirupsen/logrus"
)

func main() {
	s := loadSettings()
	if *s.version {
		fmt.Println("version 0.0.1")
		return
	}

	if *s.mode == "http" {
		httpServiceRun(s)
	} else if *s.mode == "file" {
		fileSettings(s)
	} else {
		fmt.Println("mode not found")
		return
	}

}

func httpServiceRun(s settings) {
	mfe.LogActionF("", "etl.httpServiceRun", "start")
	httpPort := *s.httpPort

	r := mfh.Route{}

	api := http.Server{
		Addr:           fmt.Sprintf(":%d", httpPort),
		Handler:        &r,
		ReadTimeout:    5e9,
		WriteTimeout:   5e9,
		MaxHeaderBytes: 16 << 20, // 16Mb
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Infof("Listen and serve %s", api.Addr)
		serverErrors <- api.ListenAndServe()
	}()

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Fatalf("Can`t start server; %v", err)

	case <-osSignals:
		log.Infof("Start shutdown...")
		ctx, cancel := context.WithTimeout(context.Background(), 5e9)
		defer cancel()

		if err := api.Shutdown(ctx); err != nil {
			log.Infof("Graceful shutdown did not complete in 5s : %v", err)
			if err := api.Close(); err != nil {
				log.Fatalf("Could not stop http server: %v", err)
			}
		}
	}
}

func fileSettings(s settings) {

	data, err := ioutil.ReadFile(*s.file)

	if err != nil {
		mfe.LogExtErrorF(err.Error(), "etl.fileSettings", "load file")
	}

	v, err := mfe.VariantNewFromJSON(string(data))

	if err != nil {
		mfe.LogExtErrorF(err.Error(), "etl.fileSettings", "parse file")
	}

	method := v.GE("method").Str()

	if method == "copy" {
		mfetl.CopyTable(v)
	}

}
