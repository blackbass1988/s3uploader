package internal

import (
	"fmt"
	"github.com/gabriel-vasile/mimetype"
	"github.com/mitchellh/goamz/s3"
	"os"
)

func tryFromFile(name string) (fmeta FileMeta, err error) {

	file, err := os.Open(name)
	if err != nil {
		return
	}

	_fileInfo, err := os.Stat(name)
	if err != nil {
		return
	}

	fmeta.Reader = file
	fmeta.Filesize = _fileInfo.Size()
	fmeta.Acl = s3.PublicRead

	fmeta.Mimetype = getContentType(name)
	if fmeta.Mimetype == "" {
		err = fmt.Errorf("%+v filesize: %d", MimeTypeNotRecognizedError, fmeta.Filesize)
		return
	}

	return
}

func getContentType(filename string) string {
	mime, err := mimetype.DetectFile(filename)

	if err != nil {
		return ""
	}

	return mime.String()
}
