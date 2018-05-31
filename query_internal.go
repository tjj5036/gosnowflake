// This includes support for a number of internal queries (such as PUT, GET, etc)
//
// For full documentation on PUT, see https://docs.snowflake.net/manuals/sql-reference/sql/put.html
// Abbreviated documentation is as follows:
// Uploads (i.e. stages) one or more data files from a local directory/folder
// on the client machine to one of the following Snowflake (i.e. internal) locations:
// - The current user’s stage.
// - The specified table’s stage.
// - Named internal stage.
// PUT <local_source> <internal_location> [ PARALLEL = <integer> ]
//                                        [ AUTO_COMPRESS = TRUE | FALSE ]
//                                        [ SOURCE_COMPRESSION = <constant> ]
// local_source takes the form:
// file://<path_to_file>/<filename>
package gosnowflake

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	PUT = "PUT"
	GET = "GET"
)

const (
	UPLOAD   = "UPLOAD"
	DOWNLOAD = "DOWNLOAD"
)

const (
	LOCATION_TYPE_LOCAL = "LOCAL_FS"
	LOCATION_TYPE_AWS   = "AWS"
	LOCATION_TYPE_AZURE = "AZURE"
)

const (
	THRESHOLD_BIG_FILE_SIZE = 16 * 1024 * 1024 // in bytes
)

// The different results that can arise during PUT / GET
const (
	RESULT_STATUS_UNKNOWN              = "Unknown status"
	RESULT_STATUS_UPLOADED             = "File uploaded"
	RESULT_STATUS_ERROR                = "Error encountered"
	RESULT_STATUS_SKIPPED              = "Skipped since file exists"
	RESULT_STATUS_NONEXIST             = "File does not exist"
	RESULT_STATUS_COLLISION            = "File name collides with another file"
	RESULT_STATUS_DIRECTORY            = "Not a file, but directory"
	RESULT_STATUS_UNSUPPORTED          = "File type not supported"
	RESULT_STATUS_UNSUPPORTED_PROVIDER = "Cloud provider not supported"
)

// Struct to hold file metadata for PUT / GET
type fileMetadata struct {
	srcFileName     string
	srcFileSize     int64
	srcCompression  string
	destFileName    string // this is the name as it will appear AFTER applying compression
	destFileSize    int64
	destCompression string
	resultStatus    string

	localDigest string

	error error  // will not be nil if there's an error, rs contains details about it
	rs    string // result status to use
}


// Determines whether the given query is a PUT or GET
// query, which are both considered to be internal queries.
// This should really work off a tokenized input or at least a list of statements.
func isPutOrGet(query string) bool {
	if len(query) < 3 {
		return false
	}
	cmd := strings.ToUpper(strings.Split(query, " ")[0])
	return cmd == PUT || cmd == GET
}

// Gets the stage location for a local stage
func getStageLocationLocal(info stageInfo) string {
	sl := info.Location
	if strings.HasPrefix(sl, "~") {
		usr, _ := user.Current()
		dir := usr.HomeDir
		sl = filepath.Join(dir, sl[1:])
	}
	return sl
}

// Struct that holds all information for an upload file task
type uploadFileTask struct {
	stageInfo stageInfo
	src       string // fully qualified src file location
	fm        fileMetadata
	error     error
	big       bool
	rs        string
}

// Uploads a single file to a stage
func (uft *uploadFileTask) uploadFile(wg *sync.WaitGroup) {
	if uft.stageInfo.LocationType == LOCATION_TYPE_LOCAL {
		stageLocation := getStageLocationLocal(uft.stageInfo)
		sfn := filepath.Join(stageLocation, uft.fm.destFileName)

		s, err := os.Open(uft.src)
		if err != nil {
			glog.V(2).Infof("Cannot open %s", uft.src)
			uft.error = err
			goto End
		}
		defer s.Close()

		err = os.MkdirAll(stageLocation, 666)
		if err != nil {
			glog.V(2).Infof("Cannot make stage location %s, err %v", stageLocation, err)
			uft.error = err
			goto End
		}

		d, err := os.Create(sfn)
		if err != nil {
			glog.V(2).Infof("Cannot create stage file %s, err %v", sfn, err)
			uft.error = err
			goto End
		}
		if _, err := io.Copy(d, s); err != nil {
			glog.V(2).Infof("Cannot copy stage file %s, err %v", sfn, err)
			d.Close() // this can also fail, but does it matter?
			uft.error = err
			goto End
		}

		err = d.Close()
		if err != nil {
			glog.V(2).Infof("Cannot close stage file %s, err %v", sfn, err)
			uft.error = err
			goto End
		}
		uft.rs = RESULT_STATUS_UPLOADED

	} else if uft.stageInfo.LocationType == LOCATION_TYPE_AWS {
		uft.rs = RESULT_STATUS_UNSUPPORTED_PROVIDER
	} else if uft.stageInfo.LocationType == LOCATION_TYPE_AZURE {
		uft.rs = RESULT_STATUS_UNSUPPORTED_PROVIDER
	}

End:
	if uft.error != nil {
		// Catch all for now.
		uft.rs = RESULT_STATUS_ERROR
		glog.Flush()
	}
	wg.Done()
}

// UploadPool is a worker group that uploads a number of files at a given / concurrency.
type UploadPool struct {
	uploadFileData []*uploadFileTask
	parallelism     int
	tasksChan      chan *uploadFileTask
	wg             sync.WaitGroup
}

// The work loop for any single goroutine.
func (p *UploadPool) work() {
	for task := range p.tasksChan {
		task.uploadFile(&p.wg)
	}
}

// Run runs all work within the pool and blocks until it's finished.
func (p *UploadPool) Run() {
	for i := 0; i < p.parallelism; i++ {
		go p.work()
	}
	p.wg.Add(len(p.uploadFileData))
	for _, task := range p.uploadFileData {
		p.tasksChan <- task
	}
	close(p.tasksChan)
	p.wg.Wait()
}

// NewPool initializes a new pool with the given tasks and parallelism
func NewPool(tasks []*uploadFileTask, parallelism int) *UploadPool {
	return &UploadPool{
		uploadFileData: tasks,
		parallelism:     parallelism,
		tasksChan:      make(chan *uploadFileTask),
	}
}

// Given a list of files as supplied to the query, resolve each file, and optionally expand a source location
// with a wildcard into multiple source locations.
// Right now, only one source will be returned from Snowflake, but this could change in the future.
//
// This also initializes a map of file metadata with the source destination location, name, and size,
// although both may be overwritten depending on the compression type. The key for the map returned is the fully
// qualified filename (absolute path), and the src file name in the file metadata value is just the name
// of the file, which is not enough to resolve the actual file on disk.
func resolveLocalFiles(givenSrcLocations []string) (map[string]fileMetadata, bool) {
	fm := make(map[string]fileMetadata)
	usr, _ := user.Current()
	dir := usr.HomeDir
	for i := 0; i < len(givenSrcLocations); i++ {
		l := givenSrcLocations[i]
		if strings.HasPrefix(l, "~") {
			l = filepath.Join(dir, l[1:])
		}

		srcLocations, err := filepath.Glob(l)
		if err != nil {
			glog.V(2).Infof("glob error: %v", err)
			glog.Flush()
			f := fileMetadata{srcFileName: l, srcFileSize: 0, rs: RESULT_STATUS_ERROR, error: err}
			fm[l] = f
			return fm, true
		}

		for i := 0; i < len(srcLocations); i++ {
			sl := srcLocations[i]
			fh, err := os.Stat(sl)
			if err != nil {
				glog.V(2).Infof("stat error: %v", err)
				glog.Flush()
				f := fileMetadata{srcFileName: fh.Name(), srcFileSize: 0, rs: RESULT_STATUS_NONEXIST, error: err}
				fm[sl] = f
				continue
			}
			if fh.IsDir() {
				glog.V(2).Infof("encountered directory %s", sl)
				glog.Flush()
				f := fileMetadata{srcFileName: fh.Name(), srcFileSize: 0, rs: RESULT_STATUS_DIRECTORY, error: err}
				fm[sl] = f
				continue
			}
			fs := fh.Size()
			f := fileMetadata{srcFileName: fh.Name(), srcFileSize: fs}
			fm[sl] = f
		}
	}
	return fm, false
}

// Computes a digest of a file. Used to resolve overwrite values.
func computeDigest(fn string) (error, string) {
	f, err := os.Open(fn)
	if err != nil {
		glog.V(2).Infof("Can no longer open file for reading %v", err)
		glog.Flush()
		return err, ""
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		glog.V(2).Infof("Cannot compute digest for %v", err)
		glog.Flush()
		return err, ""
	}
	hash := h.Sum(nil)
	digest := base64.StdEncoding.EncodeToString(hash)
	return err, digest
}

// Given a list of resolved files, determine the compression type for each file with some
// help from Snowflake. For now, just set dest = src and flush this out in another
// changeset. For now, we'll calculate the SHA256 digest which can be used to filter.
// TODO flush out various compression options
func processCompressionOptions(_ *execResponse, fm map[string]fileMetadata) {
	for k, v := range fm {
		v.srcCompression = ""
		v.destFileName = v.srcFileName

		// pretend compression happens here
		v.destFileSize = v.srcFileSize
		v.destCompression = ""

		if v.error == nil {
			err, digest := computeDigest(k)
			if err != nil {
				v.error = err
				v.rs = RESULT_STATUS_ERROR
			} else {
				v.localDigest = digest
			}
		}
		fm[k] = v
	}
}

// Given a set of files that exist locally, filter out those that need to be actually uploaded.
func filterExistingFiles(data *execResponse, fm map[string]fileMetadata) {
	for k, v := range fm {
		if data.Data.StageInfo.LocationType == LOCATION_TYPE_LOCAL {
			sl := getStageLocationLocal(data.Data.StageInfo)
			sfn := filepath.Join(sl, v.destFileName)
			if _, err := os.Stat(sfn); err == nil {
				errD, digest := computeDigest(k)
				if errD != nil {
					glog.V(2).Infof("Cannot compute digest during filter for %s", k)
					glog.Flush()
					v.error = errD
					v.rs = RESULT_STATUS_ERROR
					continue
				}

				if digest == v.localDigest {
					v.rs = RESULT_STATUS_SKIPPED
				} else {
					v.rs = RESULT_STATUS_COLLISION
				}
				fm[k] = v
			}
		}
	}
}

// If there was an error at some point in the flow, build a set of incomplete upload file tasks
// to use in processResults
func buildIncompleteUFTs(fm map[string]fileMetadata) *[]uploadFileTask {
	results := make([]uploadFileTask, 0, len(fm))
	for k, v := range fm {
		var rs = RESULT_STATUS_UNKNOWN
		if v.rs != "" {
			rs = v.rs
		}
		ud := uploadFileTask{
			src:   k,
			fm:    v,
			error: v.error,
			rs:    rs,
		}
		results = append(results, ud)
	}
	return &results
}

// Entry point for processing a PUT command. This tries to upload as many files as it can in an invocation,
// but it's possible for it to abort early (ie, invalid filename given, bad glob pattern, etc)
func execPut(data *execResponse) *snowflakeRows {
	fm, abortEarly := resolveLocalFiles(data.Data.SrcLocations)
	if abortEarly {
		ufts := buildIncompleteUFTs(fm)
		return processResults(*ufts)
	}

	processCompressionOptions(data, fm)

	if !data.Data.Overwrite {
		filterExistingFiles(data, fm)
	}

	results := uploadFiles(data, fm)
	return processResults(*results)
}

// Segregates files into "big", "small", and "ignore" files.
// Ignore in this case means filtering out files that we do NOT want uploaded due to some reason, ie
// we already uploaded it, we can't open the file for reading, etc.
func segregateFiles(fm map[string]fileMetadata) ([]string, []string, []string) {
	fb := make([]string, 0)
	fs := make([]string, 0)
	fi := make([]string, 0)
	for k, v := range fm {
		if v.error != nil || v.rs != "" {
			fi = append(fi, k)
			continue
		}
		if v.srcFileSize > THRESHOLD_BIG_FILE_SIZE {
			fb = append(fb, k)
		} else {
			fs = append(fs, k)
		}
	}
	return fb, fs, fi
}

// Processes the results. This essentially builds a rowset to return to our locally initialized
// chunk downloader.
func processResults(ufts []uploadFileTask) *snowflakeRows {
	rows := new(snowflakeRows)
	sort.Slice(ufts, func(i, j int) bool {
		return ufts[i].fm.srcFileName < ufts[j].fm.srcFileName
	})
	cc := make([][]*string, 0, len(ufts))
	for i := 0; i < len(ufts); i++ {
		uft := ufts[i]
		srcFileSize := strconv.FormatInt(uft.fm.srcFileSize, 10)
		destFileSize := strconv.FormatInt(uft.fm.destFileSize, 10)
		errorStr := ""
		if uft.error != nil {
			errorStr = uft.error.Error()
		}
		cc = append(cc, []*string{
			&uft.fm.srcFileName,
			&uft.fm.destFileName,
			&srcFileSize,
			&destFileSize,
			&uft.fm.srcCompression,
			&uft.fm.destCompression,
			&uft.rs,
			&errorStr,
		})
	}
	rt := []execResponseRowType{
		{Name: "source", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
		{Name: "target", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
		{Name: "source_size", ByteLength: 64, Length: 64, Type: "FIXED", Scale: 0, Nullable: false},
		{Name: "target_size", ByteLength: 64, Length: 64, Type: "FIXED", Scale: 0, Nullable: false},
		{Name: "source_compression", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: true},
		{Name: "target_compression", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: true},
		{Name: "status", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: true},
		{Name: "message", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: true},
	}
	cm := []execResponseChunk{}
	rows.sc = nil
	rows.RowType = rt
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:                 nil,
		ctx:                context.Background(),
		CurrentChunk:       cc,
		Total:              int64(len(cc)),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "",
		FuncDownload:       nil,
		FuncDownloadHelper: nil,
	}
	return rows
}

// Uploads uploads to a destination. Note that "big files" are done sequentially
// and small files are uploaded in parallel. However, big files are uploaded
// in chunks, and use the PARALLEL value for the parallel number of chunks that
// get uploaded.
func uploadFiles(data *execResponse, fm map[string]fileMetadata) *[]uploadFileTask {
	fb, fs, fi := segregateFiles(fm)
	stageInfo := data.Data.StageInfo

	results := make([]uploadFileTask, 0, len(fm))

	for i := 0; i < len(fi); i++ {
		ud := uploadFileTask{
			stageInfo: stageInfo,
			src:       fi[i],
			fm:        fm[fi[i]],
			error:     fm[fi[i]].error,
			rs:        fm[fi[i]].rs,
		}
		results = append(results, ud)
	}

	parallelism := 1
	for i := 0; i < len(fb); i++ {
		uploadTasks := []*uploadFileTask{
			{
				stageInfo: stageInfo,
				src:       fs[i],
				fm:        fm[fs[i]],
				error:     nil,
				big:       true,
			},
		}
		p := NewPool(uploadTasks, 1)
		p.Run()
		results = append(results, *p.uploadFileData[0])
	}

	parallelism = data.Data.Parallel
	numSmallUploads := len(fs)
	var uploadTasks []*uploadFileTask
	for i := 0; i < numSmallUploads; i++ {
		ud := uploadFileTask{
			stageInfo: stageInfo,
			src:       fs[i],
			fm:        fm[fs[i]],
			error:     nil,
			big:       false,
		}
		uploadTasks = append(uploadTasks, &ud)
	}

	p := NewPool(uploadTasks, parallelism)
	p.Run()
	for i := 0; i < numSmallUploads; i++ {
		results = append(results, *p.uploadFileData[i])
	}
	return &results
}

// Process a PUT or GET internal query.
func processPutGet(data *execResponse) *snowflakeRows {
	command := data.Data.Command
	if command == UPLOAD {
		return execPut(data)
	} else if command == DOWNLOAD {
	}
	glog.V(2).Infof("unsupported command: %v", command)
	glog.Flush()
	panic("Unsupported command")
}
