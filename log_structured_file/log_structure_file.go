package log_structured_file

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sstable-lsm-demo/segment"
)

const (
	MAX_SEGMENT_SIZE_BYTES = 1024 // 1K
)

//TODO: This is not thread-safe yet!!!!
//TODO: For simplicity sake this is not Unicode compliant
type LogStructuredFile struct {
	segments   []*segment.Segment
	dbname     string
	metadataFd *os.File
}

// TODO: dbname is the database name which will be manifested as its own directory under the root data/ dir
func NewLogStructuredFile(dbname string) *LogStructuredFile {
	dbDir := databaseRootDir(dbname)
	segments := make([]*segment.Segment, 0)
	var metadataFd *os.File

	if _, err := os.Stat(dbDir); err != nil {
		os.Mkdir(dbDir, 0666)
		metadataFd = createMetadataFile(dbDir)

		newSegmentPath := fmt.Sprintf("%s/segment0", dbDir)
		segments = append(segments, segment.NewSegment(newSegmentPath))
	} else {
		metadataPath := fmt.Sprintf("%s/metadata", dbDir)
		metadataFd, _ = os.OpenFile(metadataPath, os.O_APPEND|os.O_RDWR, os.ModeAppend)
		segments = getSegmentsFromMetadata(metadataFd, dbDir)
	}

	return &LogStructuredFile{
		segments:   segments,
		dbname:     dbname,
		metadataFd: metadataFd,
	}
}

func (lsf *LogStructuredFile) getCurrentSegment() *segment.Segment {
	return lsf.segments[len(lsf.segments)-1]
}

func (lsf *LogStructuredFile) ReadKey(key string) (string, error) {
	segment := lsf.getCurrentSegment()
	if segment.Exists(key) {
		return segment.ReadKey(key), nil
	}

	return "", errors.New("Key doesn't exist.")
}

func (lsf *LogStructuredFile) AppendKeyValue(key string, value string) {
	segment := lsf.getCurrentSegment()
	segment.AppendKeyValue(key, value)
}

func createMetadataFile(dbRoot string) *os.File {
	metadataPath := fmt.Sprintf("%s/metadata", dbRoot)
	fd, err := os.Create(metadataPath)

	if err != nil {
		fmt.Println(err)
		return nil
	}

	return fd
}

/*
Layout of metadata file
-------------------------------
segment<n>             | <n> is the segment id. The lower it is the older that segment is.
.....
segment<current>       | <current> is the id of the currently active segment. All writes are appended to this segment

Returns the list of segments in this LSF. The first one is the oldest segment. The last one is the current, active one.
*/
func getSegmentsFromMetadata(fd *os.File, rootDir string) []*segment.Segment {
	segments := make([]*segment.Segment, 0)
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		segmentFile := fmt.Sprintf("%s/%s", rootDir, scanner.Text())
		segments = append(segments, segment.NewSegment(segmentFile))
	}

	return segments
}

func databaseRootDir(dbname string) string {
	return fmt.Sprintf("%s/%s", "data", dbname)
}
