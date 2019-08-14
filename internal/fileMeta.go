package internal

import (
	"github.com/mitchellh/goamz/s3"
	"mime"
	"os"
	"path/filepath"
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
		err = MimeTypeNotRecognizedError
		return
	}

	return
}

func getContentType(filename string) string {
	return mime.TypeByExtension(filepath.Ext(filename))
}
