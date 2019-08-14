package internal

import (
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"log"
)

func GetS3Client(useHttp bool, accessKey string, secretKey string, endpoint string) (client *s3.S3) {
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

	client = s3.New(auth, region)

	return client
}
