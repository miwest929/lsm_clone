package segment

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

//TODO: This is not thread-safe yet!!!!
//TODO: For simplicity sake this is not Unicode compliant
type Segment struct {
	hashIndex map[string]int64
	fd        *os.File
}

func NewSegment(filename string) *Segment {
	var fd *os.File
	var err error

	if _, err = os.Stat(filename); os.IsNotExist(err) {
		fd, err = os.Create(filename)
	} else {
		fd, err = os.OpenFile(filename, os.O_APPEND|os.O_RDWR, os.ModeAppend)
	}

	if err != nil { //error handler
		fmt.Printf("Error when opening file %s. ErrorMsg: %s", filename, err.Error())
		return nil
	}

	segment := Segment{
		hashIndex: map[string]int64{},
		fd:        fd,
	}
	segment.loadHashMapForSegment()

	return &segment
}

func (segment *Segment) AppendKeyValue(key string, value string) {
	keyValueStr := fmt.Sprintf("%s,%s\n", key, value)
	keyValueBytes := []byte(keyValueStr)

	currLen := segment.nextOffsetValue()

	// Error occured while retrieving the next offset value
	if currLen == -1 {
		return
	}

	writer := bufio.NewWriter(segment.fd)
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

	segment.updateHashMap(key, currLen)
}

func (segment *Segment) Exists(key string) bool {
	_, ok := segment.hashIndex[key]
	return ok
}

func (segment *Segment) ReadKey(key string) string {
	offset := segment.hashIndex[key]
	_, err := segment.fd.Seek(offset, 0)
	if err != nil {
		fmt.Printf("Failed to seek to specified offset in data file. %s", err.Error())
		return ""
	}

	reader := bufio.NewReader(segment.fd)
	keyValue, _, err := reader.ReadLine()

	parts := strings.Split(string(keyValue), ",")
	return parts[1]
}

func (segment *Segment) updateHashMap(key string, offset int64) {
	segment.hashIndex[key] = offset
}

// Only called upon initialization of Segment object
func (segment *Segment) loadHashMapForSegment() {
	var offset int64 = 0
	scanner := bufio.NewScanner(segment.fd)
	for scanner.Scan() {
		rawBytes := scanner.Bytes()
		keyValue := string(rawBytes)
		parts := strings.Split(string(keyValue), ",")
		segment.updateHashMap(parts[0], offset)

		offset += int64(len(rawBytes)) + 1
	}
}

func (segment *Segment) nextOffsetValue() int64 {
	info, err := segment.fd.Stat()

	if err != nil {
		fmt.Println(err)
		return -1
	}

	return info.Size()
}
