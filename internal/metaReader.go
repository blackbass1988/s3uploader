package internal

import (
	"errors"
	"github.com/mitchellh/goamz/s3"
	"io"
	"net/url"
)

type FileMeta struct {
	Reader   io.ReadCloser
	Filesize int64
	Mimetype string
	Acl      s3.ACL
}

var MimeTypeNotRecognizedError = errors.New("mime type not recognized")
var FileInvalidSizeError = errors.New("filesize has a invalid size")

func NewMeta(sourceIsS3 bool, name string, sourceS3Bucket *s3.Bucket) (fmeta FileMeta, err error) {
	var u *url.URL

	if sourceIsS3 {
		u, err = url.Parse(name)
		if err != nil {
			return
		}
		fmeta, err = tryFromUrl(u, sourceS3Bucket)
		return
	}

	fmeta, err = tryFromFile(name)

	return
}
