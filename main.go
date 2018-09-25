package main

import (
	"flag"
	"fmt"
)

func main() {
	var version = flag.Bool("version", false, "return current version")
	var verbose = flag.Bool("v", false, "verbose (default 0)\n1 - verbose\nother not verbose")
	var mode = flag.String("m", "", `mode (default o)
o - one action
qcp - LoadAndInsertFromQuery params (dtf, dfcs, dtt, dtcs, q, tt)`)

	var dtf = flag.String("dtf", "", "dbTypeFrom")
	var dfcs = flag.String("dfcs", "", "dbFromCS")
	var dtt = flag.String("dtt", "", "dbTypeTo")
	var dtcs = flag.String("dtcs", "", "dbToCS")
	var q = flag.String("q", "", "queryFrom")
	var tt = flag.String("tt", "", "tableTo")

	flag.Parse()

	if *version {
		fmt.Println("version 0.0.1")
		return
	}

	if *verbose {
		fmt.Println("mode: " + *mode)
		fmt.Println("dtf: " + *dtf)
		fmt.Println("dfcs: " + *dfcs)
		fmt.Println("dtt: " + *dtt)
		fmt.Println("dtcs: " + *dtcs)
		fmt.Println("q: " + *q)
		fmt.Println("tt: " + *tt)
	}

	if *mode == "qcp" {
		err := LoadAndInsertFromQuery(*dtf, *dfcs, *dtt, *dtcs, *q, *tt, *verbose)
		if err != nil {
			fmt.Println(err)
		}
	}

}
