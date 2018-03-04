package log_structured_file

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

//TODO: This is not thread-safe yet!!!!
type LogStructuredFile struct {
	hashIndex map[string]int64
	fd        *os.File
}

// TODO: dbname is the database name which will be manifested as its own directory under the root data/ dir
func NewLogStructuredFile(filename string) *LogStructuredFile {
	var fd *os.File
	var err error

	if _, err = os.Stat(filename); os.IsNotExist(err) {
		fd, err = os.Create(filename) //, os.O_APPEND|os.O_RDWR)
	} else {
		fd, err = os.OpenFile(filename, os.O_APPEND|os.O_RDWR, os.ModeAppend)
	}

	if err != nil { //error handler
		fmt.Printf("Error when opening file %s. ErrorMsg: %s", filename, err.Error())
		return nil
	}

	return &LogStructuredFile{
		hashIndex: map[string]int64{},
		fd:        fd,
	}
}

func (lsf *LogStructuredFile) AppendKeyValue(key string, value string) {
	keyValueStr := fmt.Sprintf("%s,%s\n", key, value)
	keyValueBytes := []byte(keyValueStr)

	currLen := lsf.nextOffsetValue()

	// Error occured while retrieving the next offset value
	if currLen == -1 {
		return
	}

	writer := bufio.NewWriter(lsf.fd)
	_, err := writer.Write(keyValueBytes)

	if err != nil {
		fmt.Println(err)
		return
	}

	err = writer.Flush()

	if err != nil {
		fmt.Println(err)
		return
	}

	lsf.updateHashMap(key, currLen)
}

func (lsf *LogStructuredFile) ReadKey(key string) (string, string) {
	offset := lsf.hashIndex[key]

	_, err := lsf.fd.Seek(offset, 0)
	if err != nil {
		fmt.Printf("Failed to seek to specified offset in data file. %s", err.Error())
		return "", ""
	}

	reader := bufio.NewReader(lsf.fd)
	keyValue, _, err := reader.ReadLine()

	parts := strings.Split(string(keyValue), ",")
	return parts[0], parts[1]
}

func (lsf *LogStructuredFile) updateHashMap(key string, offset int64) {
	lsf.hashIndex[key] = offset
}

func (lsf *LogStructuredFile) nextOffsetValue() int64 {
	info, err := lsf.fd.Stat()

	if err != nil {
		fmt.Println(err)
		return -1
	}

	return info.Size()
}
