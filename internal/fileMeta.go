package internal

import (
	"fmt"
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

	f, err := os.Open(name)
	if err != nil {
		return
	}
	defer f.Close()

	fmeta.Mimetype, err = getContentType(f)

	if err != nil {
		err = fmt.Errorf("%+v filesize: %d, err: %+v", MimeTypeNotRecognizedError, fmeta.Filesize, err)
		return
	}

	if fmeta.Mimetype == "" {
		err = fmt.Errorf("%+v filesize: %d", MimeTypeNotRecognizedError, fmeta.Filesize)
		return
	}

	return
}
