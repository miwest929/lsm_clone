package main

import (
	//	"fmt"
	"sstable-lsm-demo/log_structured_file"
)

func main() {
	lsf := log_structured_file.NewLogStructuredFile("data/segment0")
	lsf.AppendKeyValue("message", "Ignore")
}
