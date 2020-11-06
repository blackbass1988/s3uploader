package internal

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/mitchellh/goamz/s3"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var NotSuccessHttpStatusError = errors.New("url returned not 200")
var NotImplementedAclMappingError = errors.New("mapping not implemented")

func tryFromUrl(u *url.URL, sourceS3Bucket *s3.Bucket) (fmeta FileMeta, err error) {
	key := u.Path

	resp, err := sourceS3Bucket.GetResponse(key)

	if err != nil {
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = NotSuccessHttpStatusError
		return
	}

	fmeta.Reader = resp.Body

	filesize, err := strconv.ParseInt(resp.Header.Get("content-length"), 10, 0)

	if filesize == 0 {
		err = fmt.Errorf("%+v; size: %d", FileInvalidSizeError, filesize)
		return
	}

	contentType := resp.Header.Get("content-type")

	if contentType == "text/plain" || contentType == "" {
		var buf, _ = ioutil.ReadAll(resp.Body)
		contentType, err = getContentType(bytes.NewBuffer(buf))

		if err != nil {
			err = fmt.Errorf("%+v size: %d; err: %+v", MimeTypeNotRecognizedError, filesize, err)
			return
		}

		fmeta.Reader = ioutil.NopCloser(bytes.NewReader(buf))
	}

	if contentType == "" {
		err = fmt.Errorf("%+v size: %d", MimeTypeNotRecognizedError, filesize)
		return
	}

	acl, err := getAcl(sourceS3Bucket, key, u)

	if err != nil {
		return
	}

	fmeta.Filesize = filesize
	fmeta.Mimetype = contentType
	fmeta.Acl = acl

	return
}

func getAcl(s3Bucket *s3.Bucket, key string, u *url.URL) (acl s3.ACL, err error) {
	var cephAclResponse AccessControlPolicy
	acl = s3.Private

	params := make(map[string][]string)
	params["acl"] = []string{""}
	headers := make(map[string][]string)
	headers["Host"] = []string{s3Bucket.S3.S3BucketEndpoint}
	headers["Date"] = []string{time.Now().In(time.UTC).Format(time.RFC1123)}

	toSignString := key
	if toSignString[0:1] != "/" {
		toSignString = "/" + toSignString
	}

	toSignString = "/" + s3Bucket.Name + toSignString

	sign(s3Bucket.S3.Auth, "GET", toSignString, params, headers)

	z := s3Bucket.URL(key)
	u, _ = url.Parse(z)
	u.RawQuery = "acl"
	hreq := http.Request{
		URL:        u,
		Method:     "GET",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Close:      true,
		Header:     headers,
	}
	ok, err := s3Bucket.HTTPClient().Do(&hreq)
	if err != nil {
		return
	}

	if ok.StatusCode != http.StatusOK {
		err = NotSuccessHttpStatusError
		return
	}

	decoder := xml.NewDecoder(ok.Body)

	err = decoder.Decode(&cephAclResponse)
	if err != nil {
		return
	}

	for _, g := range cephAclResponse.AccessControlList.Grants {
		if g.Gruntee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
			if g.Permission == "READ" {
				acl = s3.PublicRead
			}

			if g.Permission == "WRITE" {
				acl = s3.PublicReadWrite
			}
			break
		}

		if g.Gruntee.URI == "http://acs.amazonaws.com/groups/global/AuthenticatedUsers" {
			err = NotImplementedAclMappingError
		}

		if g.Gruntee.URI == "http://acs.amazonaws.com/groups/s3/LogDelivery" {
			err = NotImplementedAclMappingError
		}

		if g.Permission == "FULL_CONTROL" && g.Gruntee.URI != "" {
			err = NotImplementedAclMappingError
		}

	}

	return
}
