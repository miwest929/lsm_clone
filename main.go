package main

import (
	"fmt"
	"sstable-lsm-demo/log_structured_file"
)

func main() {
	lsf := log_structured_file.NewLogStructuredFile("demo")
	//	lsf.AppendKeyValue("cpuUtil", "199")
	//	lsf.AppendKeyValue("message", "Idlement")
	//	lsf.AppendKeyValue("diskQueueEffect", "heavy")
	value, _ := lsf.ReadKey("message")
	fmt.Printf("value = %s\n", value)
}
