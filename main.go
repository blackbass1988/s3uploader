package main

import (
	"bufio"
	"github.com/blackbass1988/s3uploader/internal"
	"github.com/mitchellh/goamz/s3"
	"io"

	"flag"
	"fmt"
	"log"
	"os"
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
	MaxProcCount   int        //max proc mount

	messages chan *Message

	activePool chan bool //active concurrent uploads

	destinationAccessKey, destinationSecretKey, destinationEndpoint string
	sourceAccessKey, sourceSecretKey, sourceEndpoint                string

	destinationBucketName, sourceBucketName string

	errorLog string //filename of error log

	inputFile, removeThisStringFromKey                                              string
	profile, silent, useHttp, createBucket, sourceIsS3, trimAfterQuestionSignOnSave bool

	//	stats_putBytes uint64 = 0
)

const (
	version string = "2.0.0"
)

var (
	destClient   *s3.S3
	sourceClient *s3.S3
)

func main() {

	var (
		curRSize, curTotalSize, curSize, curTotalTransferred uint64
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

	flag.StringVar(&destinationBucketName, "destination-bucket", "", "destination bucket name")
	flag.StringVar(&sourceBucketName, "source-bucket", "", "source bucket name. Use destination if empty")

	flag.StringVar(&destinationAccessKey, "destination-access-key", "", "destination access key")
	flag.StringVar(&destinationSecretKey, "destination-secret-key", "", "destination secret key")
	flag.StringVar(&destinationEndpoint, "destination-endpoint", "", "destination endpoint")

	flag.StringVar(&sourceAccessKey, "source-access-key", "", "source access key. Use destination if empty")
	flag.StringVar(&sourceSecretKey, "source-secret-key", "", "source secret key. Use destination if empty")
	flag.StringVar(&sourceEndpoint, "source-endpoint", "", "source endpoint. Use destination if empty")

	flag.Uint64Var(&offset, "offset", uint64(0), "count of lines to skip before start upload")

	flag.IntVar(&MaxProcCount, "max-proc", 1, "max proc count")
	flag.IntVar(&maxRoutineSize, "c", 20, "concurrency")

	flag.BoolVar(&silent, "silent", false, "minimalizing logs")
	flag.BoolVar(&profile, "profile", false, "save profiling to profile.prof on exit")
	flag.BoolVar(&useHttp, "use-http", false, "use http instead https")
	flag.BoolVar(&createBucket, "create-bucket", false, "create bucket if it not exists")
	flag.BoolVar(&trimAfterQuestionSignOnSave, "trim-question-sign", false, "removes char \"?\" and after on save")
	flag.Parse()

	/// eo parse args

	if profile {
		fCpuProfiling, err := os.Create("profile.prof")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(fCpuProfiling)
		defer func() {
			pprof.StopCPUProfile()
		}()

	}
	if inputFile == "" || destinationAccessKey == "" || destinationSecretKey == "" || destinationBucketName == "" {
		fmt.Println("One of this parameters is empty:\n input file, access key, secret key, prefix, bucket name")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if sourceEndpoint != "" {
		sourceIsS3 = true
		fmt.Println("sourceBucketName and sourceEndpoint is set. Will be use s3-s3 copy mode")
	}

	if sourceBucketName == "" {
		sourceBucketName = destinationBucketName
		fmt.Println("sourceBucketName not set. Use destinationBucketName")
	}

	if sourceAccessKey == "" {
		sourceAccessKey = destinationAccessKey
		fmt.Println("sourceAccessKey not set. Use destinationAccessKey")
	}

	if sourceSecretKey == "" {
		sourceSecretKey = destinationSecretKey
		fmt.Println("sourceSecretKey not set. Use destinationSecretKey")
	}

	runtime.GOMAXPROCS(MaxProcCount)

	destClient = getDestinationS3Client()
	sourceClient = getSourceS3Client()

	checkAndCreateBucket(destClient, destinationBucketName, destinationEndpoint)
	checkAndCreateBucket(sourceClient, sourceBucketName, sourceEndpoint)

	messages = make(chan *Message, maxRoutineSize*2)
	activePool = make(chan bool, maxRoutineSize)

	go saveToBucketFromFile(inputFile, removeThisStringFromKey, destinationBucketName)

	work(curRSize, curTotalSize, curSize, curTotalTransferred)
}

func checkAndCreateBucket(s3Client *s3.S3, bucketName string, endpoint string) {

	bucketFound := findBucket(s3Client, bucketName)

	if !bucketFound && createBucket {

		bucket := s3Client.Bucket(bucketName)
		err := bucket.PutBucket(s3.PublicRead)
		if err != nil {
			panic(err)
		}
		bucketFound = findBucket(s3Client, bucketName)
	}

	if !bucketFound {
		log.Fatalln(fmt.Printf("FATAL! Bucket \"%s\" not found for S3 endpoint \"%s\". Maybe you need to use -create-bucket=true option.", bucketName, endpoint))
	}
}

func work(curRSize uint64, curTotalSize uint64, curSize uint64, curTotalTransferred uint64) {

	appRunning := false

	errorLogFile, err := os.OpenFile(errorLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer errorLogFile.Close()

	if err != nil {
		log.Fatalln("ERROR while file open", err)
		os.Exit(2)
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
		//curPutBytes = atomic.LoadUint64(&stats_putBytes)

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
			if profile {
				runtime.ReadMemStats(m)
				log.Printf("~ Goroutines count %d\n", runtime.NumGoroutine())
				log.Printf("~ Memory HeapAlloc %d\n", m.HeapAlloc/1024)
				log.Printf("~ Memory HeapInuse %d\n", m.HeapInuse/1024)

				log.Printf("~ Memory Alloc %d\n", m.Alloc)
				log.Printf("~ Memory Mallocs %d\n", m.Mallocs)
				log.Printf("~ Memory Frees %d\n", m.Frees)
			}
			//atomic.SwapUint64(&stats_putBytes, uint64(0))

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

func saveToBucketFromFile(file string, prefixToTrim string, destBucket string) {
	var err error
	var reader *bufio.Reader
	var buffer []byte
	var f *os.File
	var fileSource, key string
	var offsetDone bool

	f, err = os.Open(file)

	if err != nil {
		log.Fatalln("error while open", file, err)
	}

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
		key = strings.Replace(fileSource, prefixToTrim, "", -1)

		activePool <- true
		go uploadToS3(destBucket, fileSource, key, activePool)

		fileSource = ""
		key = ""

	}

}

func uploadToS3(destinationBucketName string, source string, key string, activePool chan bool) {

	var (
		destinationBucket *s3.Bucket
		startTime         int64
		filesize          uint64

		fmeta    internal.FileMeta
		mimeType string

		err error
	)

	if !silent {
		startTime = time.Now().Unix()
	}

	defer func() {
		if !silent {
			messages <- &Message{fmt.Sprintf("\"%s\" -> \"%s\" done. Time elapsed %d sec", source, key, time.Now().Unix()-startTime), "", nil}
		}

		atomic.AddUint64(&totalTransferred, filesize)
		atomic.AddUint64(&currentRoutineSize, ^uint64(0))
		atomic.AddUint64(&fileCount, uint64(1))

		err = nil
		destinationBucket = nil

		if r := recover(); r != nil {
			fmt.Println("Recovered in uploadToS3", r)
		}
		<-activePool
	}()

	destinationBucket = getDestinationS3Client().Bucket(destinationBucketName)
	sourceBucket := getSourceS3Client().Bucket(sourceBucketName)

	if fmeta, err = internal.NewMeta(sourceIsS3, source, sourceBucket); err == nil {

		defer fmeta.Reader.Close()
		_reader := bufio.NewReader(fmeta.Reader)

		filesize = uint64(fmeta.Filesize)
		mimeType = fmeta.Mimetype
		err = destinationBucket.PutReader(key, _reader, fmeta.Filesize, mimeType, fmeta.Acl)

		if err != nil {
			messages <- &Message{"", source, err}
			return
		}

	} else {
		messages <- &Message{"", source, err}
	}
}

func getDestinationS3Client() (client *s3.S3) {
	if destClient != nil {
		return destClient
	}

	destClient = internal.GetS3Client(useHttp, destinationAccessKey, destinationSecretKey, destinationEndpoint, maxRoutineSize)
	return destClient
}

func getSourceS3Client() (client *s3.S3) {
	if sourceClient != nil {
		return sourceClient
	}
	sourceClient = internal.GetS3Client(useHttp, sourceAccessKey, sourceSecretKey, sourceEndpoint, maxRoutineSize)
	return sourceClient
}

func findBucket(client *s3.S3, bucketName string) bool {

	var _b s3.Bucket

	log.Println("reading list of buckets of", client.Region.S3Endpoint)
	resp, err := client.ListBuckets()
	bucketFound := false

	if err != nil {
		log.Fatalln("FATAL! Error while reading list of buckets:", err)
	}

	if len(resp.Buckets) == 0 {
		log.Fatalln("FATAL! No buckets found for S3 endpoint", destinationEndpoint)
	}

	for _, _b = range resp.Buckets {

		if _b.Name == bucketName {
			bucketFound = true
		}
	}

	return bucketFound

}
