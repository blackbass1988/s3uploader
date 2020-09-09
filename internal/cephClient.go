package internal

import (
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"log"
	"net"
	"net/http"
	"time"
)

func GetS3Client(useHttp bool, accessKey string, secretKey string, endpoint string, maxRoutineSize int) (client *s3.S3) {
	var auth aws.Auth
	var region aws.Region
	var schema string

	auth = aws.Auth{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	if useHttp {
		schema = "http"
	} else {
		schema = "https"
	}

	region = aws.Region{
		Name:                 "squid1",                  //canonical name
		S3Endpoint:           schema + "://" + endpoint, //address
		S3LocationConstraint: true,
		S3LowercaseBucket:    true,
	}

	log.Printf("Connecting to %s...\n", region.S3Endpoint)

	maxIdleConns := maxRoutineSize
	connectTimeout := 1 * time.Second

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   connectTimeout,
				KeepAlive: 30 * time.Minute,
			}).DialContext,
			MaxIdleConns:          maxIdleConns,
			IdleConnTimeout:       30 * time.Minute,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}}

	client = s3.New(auth, region)
	client.HTTPClient = func() *http.Client {
		return httpClient
	}

	return client
}
