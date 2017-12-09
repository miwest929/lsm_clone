package main

import (
	"fmt"
	"sstable-lsm-demo/log_structured_file"
)

func main() {
	fmt.Println("Demo")

	lsf := log_structured_file.NewLogStructuredFile()
	lsf.AppendKeyValue("message", "Ignore")
}
