package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/myfantasy/mfe"
	log "github.com/sirupsen/logrus"
)

type settings struct {
	version  *bool
	verbose  *bool
	logLevel *int
	httpPort *int
	mode     *string
	file     *string
}

func loadSettings() (s settings) {
	s.version = flag.Bool("version", false, "return current version")
	s.verbose = flag.Bool("v", false, "verbose (default 0)\n1 - verbose\nother not verbose")
	s.logLevel = flag.Int("loglevel", int(log.InfoLevel), "loglevel (default 4)\n0 - PanicLevel\n1 - FatalLevel\n2 - ErrorLevel\n3 - WarnLevel\n4 - InfoLevel\n5 - DebugLevel")
	s.mode = flag.String("m", "http", `mode (default http)
	http - http service
	file - load file and `)
	s.httpPort = flag.Int("port", 8080, "Http Port (default 8080)")
	s.file = flag.String("fn", "settings.json", `file name for file mode (default settings.json)`)

	flag.Parse()

	log.SetLevel(log.Level(*s.logLevel))

	if *s.verbose {
		mfe.ActionLevelSet(log.InfoLevel)
		fmt.Println("Started")
		fmt.Println(time.Now())
		fmt.Println("Mode:", *s.mode)
		fmt.Println("loglevel:", *s.logLevel)
		fmt.Println("port:", *s.httpPort)
		fmt.Println("fn:", *s.file)
	}
	return s
}
