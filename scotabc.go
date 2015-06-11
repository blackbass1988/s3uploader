package main

import (
	"bufio"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"io"
	//	"io/ioutil"
	"errors"
	"flag"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"
)

var (
	currentRoutineSize uint64 = 0
	fileTotal          uint64 = 0
	fileCount          uint64 = 0
	totalTransferred   uint64 = 0

	offset         uint64 = 0 //how many lines need to skip before start uploading
	maxRoutineSize int        //concurrency
	MAX_PROC_COUNT int        //max proc mount

	messages chan *Message

	activePool chan bool //active concurent uploads

	s3AccessKey, s3SecretKey, s3Endpoint string
	errorLog                             string //filename of error log

	inputFile, removeThisStringFromKey, bucketName string
	profile, silent, useHttp, createBucket         bool

//	stats_putBytes uint64 = 0
)

const (
	version string = "1.0.1"
)

func main() {

	var (
		client                                               *s3.S3
		err                                                  error
		curRSize, curTotalSize, curSize, curTotalTransferred uint64
		//	, curPutBytes uint64
		errorLogFile            *os.File
		bucketFound, appRunning bool = false, false
	)

	fmt.Println("")
	fmt.Println("~")
	fmt.Println("~ СоЗФНЭСПГОРНСД", version)
	fmt.Println("~ СалионоваОлегаЗагружающийФайлыНаЭс3ПосредствомГоРутинНаСервераДрома", version)
	fmt.Println("~")
	fmt.Println("")

	/// parse args

	flag.StringVar(&errorLog, "error-log", "error.log", "save errors to this file")
	flag.StringVar(&inputFile, "i", "", "input file")
	flag.StringVar(&removeThisStringFromKey, "p", "", "removes this string from key on PUT")
	flag.StringVar(&bucketName, "b", "ssd1", "bucket name")
	flag.StringVar(&s3AccessKey, "s3-access-key", "", "s3 access key")
	flag.StringVar(&s3SecretKey, "s3-secret-key", "", "s3 secret key")
	flag.StringVar(&s3Endpoint, "s3-endpoint", "", "s3 endpoint")

	flag.Uint64Var(&offset, "offset", uint64(0), "count of lines to skip before start upload")

	flag.IntVar(&MAX_PROC_COUNT, "max-proc", 1, "max proc count")
	flag.IntVar(&maxRoutineSize, "c", 20, "concurency")

	flag.BoolVar(&silent, "silent", false, "minimalizing logs")
	flag.BoolVar(&profile, "profile", false, "save profiling to profile.prof on exit")
	flag.BoolVar(&useHttp, "use-http", false, "use http instead https")
	flag.BoolVar(&createBucket, "create-bucket", false, "create bucket if it not exists")

	flag.Parse()

	/// eo parse args

	if profile {
		f_cpu_profiling, err := os.Create("profile.prof")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f_cpu_profiling)
		defer func() {
			pprof.StopCPUProfile()
		}()

	}

	if inputFile == "" || s3AccessKey == "" || s3SecretKey == "" || bucketName == "" {
		fmt.Println("One of this parameters is empty:\n input file, access key, secret key, prefix, bucket name")
		flag.PrintDefaults()
		os.Exit(0)
	}

	runtime.GOMAXPROCS(MAX_PROC_COUNT)

	messages = make(chan *Message, maxRoutineSize*2)
	activePool = make(chan bool, maxRoutineSize)

	client = getS3Client()

	bucketFound = findBucket(client)

	if !bucketFound && createBucket {
		var bucket *s3.Bucket

		bucket = client.Bucket(bucketName)
		err = bucket.PutBucket(s3.PublicRead)
		if err != nil {
			panic(err)
		}
		bucketFound = findBucket(client)
	}

	if !bucketFound {
		log.Fatalln(fmt.Printf("FATAL! Bucket \"%s\" not found for S3 endpoint \"%s\". Maybe you need to use -create-bucket=true option.", bucketName, s3Endpoint))
	}

	go saveToBucketFromFile(inputFile, removeThisStringFromKey, bucketName, client)

	errorLogFile, err = os.OpenFile(errorLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer errorLogFile.Close()

	if err != nil {
		log.Fatalln(err)
	}

	m := &runtime.MemStats{}
	c := time.Tick(1 * time.Second)
	cProfile := time.Tick(30 * time.Second)
	var message *Message

	for {
		curRSize = atomic.LoadUint64(&currentRoutineSize)
		curTotalSize = atomic.LoadUint64(&fileTotal)
		curSize = atomic.LoadUint64(&fileCount)
		curTotalTransferred = atomic.LoadUint64(&totalTransferred)
		//		curPutBytes = atomic.LoadUint64(&stats_putBytes)

		select {
		case message = <-messages:
			if message.Error != nil {
				log.Println("ERROR: ", message.Error)
				_, err = errorLogFile.WriteString(fmt.Sprintf("%s ### %s ### %s\n", time.Now(), message.SourceLine, message.Error))

				if err != nil {
					log.Fatalln("FATAL! ", err)
				}
			}

			if message.String != "" && !silent {
				log.Println(message.String)
			}

			if !appRunning {
				appRunning = true
			}

		case <-c:
			//runtime.GC()
			if profile {
				runtime.ReadMemStats(m)
				log.Printf("~ Goroutines count %d\n", runtime.NumGoroutine())
				log.Printf("~ Memory HeapAlloc %d\n", m.HeapAlloc/1024)
				log.Printf("~ Memory HeapInuse %d\n", m.HeapInuse/1024)

				log.Printf("~ Memory Alloc %d\n", m.Alloc)
				log.Printf("~ Memory Mallocs %d\n", m.Mallocs)
				log.Printf("~ Memory Frees %d\n", m.Frees)
			}
			//			atomic.SwapUint64(&stats_putBytes, uint64(0))

			log.Printf("~ Processing %d/%d;\n", curSize, curTotalSize)

			if curTotalTransferred > 1073741824 { //gb
				log.Printf("~ Transferred %.2f GB\n", float32(curTotalTransferred)/1073741824)
			} else if curTotalTransferred > 1048576 { //mb
				log.Printf("~ Transferred %.2f MB\n", float32(curTotalTransferred)/1048576)
			} else if curTotalTransferred > 1024 { //kb
				log.Printf("~ Transferred %.2f KB\n", float32(curTotalTransferred)/1024)
			} else { //b
				log.Printf("~ Transferred %d B\n", curTotalTransferred)
			}

			if appRunning && curRSize == uint64(0) && curSize == curTotalSize {
				os.Exit(0)
			}

			if profile {

			}
		case <-cProfile:
			if profile {
				var f_heap_profiling io.Writer
				f_heap_profiling, err = os.Create("profile_heap.prof")
				pprof.WriteHeapProfile(f_heap_profiling)
			}
		}

	}
}

type Message struct {
	String     string
	SourceLine string
	Error      error
}

func saveToBucketFromFile(file string, root string, bucket string, client *s3.S3) {
	var err error
	var reader *bufio.Reader
	var buffer []byte
	var f *os.File
	var fileSource, key string
	var offsetDone bool

	f, err = os.Open(file)

	defer func() {
		err = nil
		reader = nil
		buffer = []byte{}
		fileSource = ""
		key = ""

		if r := recover(); r != nil {
			fmt.Println("Recovered in saveToBucketFromFile", r)
		}

		f.Close()
	}()

	reader = bufio.NewReader(f)

	for {
		buffer, _, err = reader.ReadLine()

		if err == io.EOF {
			err = nil
			messages <- &Message{fmt.Sprintf("File \"%s\" read!", inputFile), "", err}
			break
		} else if err != nil {
			messages <- &Message{"", "", err}
			break
		}

		atomic.AddUint64(&fileTotal, uint64(1))

		if !offsetDone {

			if atomic.LoadUint64(&offset) > 0 {
				atomic.AddUint64(&fileCount, uint64(1))
				atomic.AddUint64(&offset, ^uint64(0))
				continue
			} else {
				offsetDone = true
			}
		}

		atomic.AddUint64(&currentRoutineSize, uint64(1))
		fileSource = string(buffer)
		key = strings.Replace(fileSource, root, "", -1)

		activePool <- true
		go uploadToS3(client, bucket, fileSource, key, activePool)

		fileSource = ""
		key = ""

	}

}

func uploadToS3(client *s3.S3, bucket string, source string, key string, activePool chan bool) (err error) {

	var (
		_bucket   *s3.Bucket
		startTime int64
		data      []byte
		filesize  uint64
	)

	if !silent {
		startTime = time.Now().Unix()
	}
	defer func() {
		if !silent {
			messages <- &Message{fmt.Sprintf("\"%s\" -> \"%s\" done. Time elapsed %d sec", source, key, time.Now().Unix()-startTime), "", nil}
		}

		//atomic.AddUint64(&stats_putBytes, filesize)
		atomic.AddUint64(&totalTransferred, filesize)
		atomic.AddUint64(&currentRoutineSize, ^uint64(0))
		atomic.AddUint64(&fileCount, uint64(1))

		err = nil
		_bucket = nil
		data = []byte{}

		if r := recover(); r != nil {
			fmt.Println("Recovered in uploadToS3", r)
		}
		<-activePool
	}()

	var (
		_f        *os.File
		_fileInfo os.FileInfo
		mimeType  string
	)

	if _f, err = os.Open(source); err == nil {

		defer _f.Close()
		_reader := bufio.NewReader(_f)

		_fileInfo, err = os.Stat(source)
		if err != nil {
			messages <- &Message{"", source, err}
			return err
		}
		filesize = uint64(_fileInfo.Size())
		_bucket = client.Bucket(bucket)
		mimeType = getContentType(source)

		if mimeType == "" {
			messages <- &Message{"", source, errors.New("mime type not recognized")}
		}
		err = _bucket.PutReader(key, _reader, _fileInfo.Size(), mimeType, s3.PublicRead)

		if err != nil {
			messages <- &Message{"", source, err}
			return err
		}

	} else {
		messages <- &Message{"", source, err}

		return err
	}

	return err

}

func getContentType(filename string) string {
	return mime.TypeByExtension(filepath.Ext(filename))
}

func getS3Client() (client *s3.S3) {
	var auth aws.Auth
	var region aws.Region
	var schema string

	auth = aws.Auth{
		s3AccessKey,
		s3SecretKey,
		"",
	}

	if useHttp {
		schema = "http"
	} else {
		schema = "https"
	}

	region = aws.Region{
		"squid1", //canonical name
		"",
		schema + "://" + s3Endpoint, //address
		"",
		true,
		true,
		"",
		"",
		"",
		"",
		"",
		"",
		"",
		"",
	}

	log.Printf("Connecting to %s...\n", region.S3Endpoint)

	client = s3.New(auth, region)

	return client
}

func findBucket(client *s3.S3) bool {

	var _b s3.Bucket

	log.Println("reading list of buckets...")
	resp, err := client.ListBuckets()
	bucketFound := false

	if err != nil {
		log.Fatalln("FATAL! Error while reading list of buckets: ", err)
		os.Exit(2)
	}

	if len(resp.Buckets) == 0 {
		log.Fatalln("FATAL! No buckets found for S3 endpoint", s3Endpoint)
	}

	for _, _b = range resp.Buckets {

		if _b.Name == bucketName {
			bucketFound = true
		}
	}

	return bucketFound

}
