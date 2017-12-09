package log_structured_file

import (
	"bytes"
)

type LogStructuredFile struct {
	buffer    bytes.Buffer
	hashIndex map[string]int
}

func NewLogStructuredFile() *LogStructuredFile {
	return &LogStructuredFile{
		hashIndex: make(map[string]int),
	}
}

func (lsf *LogStructuredFile) AppendKeyValue(key string, value string) (int, error) {
	return 10, nil
}

func (lsf *LogStructuredFile) Write(fileName string) error {
	return nil
}

func (lsf *LogStructuredFile) Read(fileName string) error {
	return nil
}

func (lsf *LogStructuredFile) updateHashMap(key string, offset int) {
}
