package internal

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"github.com/mitchellh/goamz/aws"
	"log"
	"sort"
	"strings"
)

var debug = false
var b64 = base64.StdEncoding

var s3ParamsToSign = map[string]bool{
	"acl":                          true,
	"delete":                       true,
	"location":                     true,
	"logging":                      true,
	"notification":                 true,
	"partNumber":                   true,
	"policy":                       true,
	"requestPayment":               true,
	"torrent":                      true,
	"uploadId":                     true,
	"uploads":                      true,
	"versionId":                    true,
	"versioning":                   true,
	"versions":                     true,
	"response-content-type":        true,
	"response-content-language":    true,
	"response-expires":             true,
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
}

func sign(auth aws.Auth, method, canonicalPath string, params, headers map[string][]string) {
	var md5, ctype, date, xamz string
	var xamzDate bool
	var sarray []string

	// add security token
	if auth.Token != "" {
		headers["x-amz-security-token"] = []string{auth.Token}
	}

	if auth.SecretKey == "" {
		// no auth secret; skip signing, e.g. for public read-only buckets.
		return
	}

	for k, v := range headers {
		k = strings.ToLower(k)
		switch k {
		case "content-md5":
			md5 = v[0]
		case "content-type":
			ctype = v[0]
		case "date":
			if !xamzDate {
				date = v[0]
			}
		default:
			if strings.HasPrefix(k, "x-amz-") {
				vall := strings.Join(v, ",")
				sarray = append(sarray, k+":"+vall)
				if k == "x-amz-date" {
					xamzDate = true
					date = ""
				}
			}
		}
	}
	if len(sarray) > 0 {
		sort.StringSlice(sarray).Sort()
		xamz = strings.Join(sarray, "\n") + "\n"
	}

	expires := false
	if v, ok := params["Expires"]; ok {
		// Query string request authentication alternative.
		expires = true
		date = v[0]
		params["AWSAccessKeyId"] = []string{auth.AccessKey}
	}

	sarray = sarray[0:0]
	for k, v := range params {
		if s3ParamsToSign[k] {
			for _, vi := range v {
				if vi == "" {
					sarray = append(sarray, k)
				} else {
					// "When signing you do not encode these values."
					sarray = append(sarray, k+"="+vi)
				}
			}
		}
	}

	canonicalPath = amazonEscape(canonicalPath)

	if len(sarray) > 0 {
		sort.StringSlice(sarray).Sort()
		canonicalPath = canonicalPath + "?" + strings.Join(sarray, "&")
	}

	payload := method + "\n" + md5 + "\n" + ctype + "\n" + date + "\n" + xamz + canonicalPath
	hash := hmac.New(sha1.New, []byte(auth.SecretKey))
	hash.Write([]byte(payload))
	signature := make([]byte, b64.EncodedLen(hash.Size()))
	b64.Encode(signature, hash.Sum(nil))

	if expires {
		params["Signature"] = []string{string(signature)}
	} else {
		headers["Authorization"] = []string{"AWS " + auth.AccessKey + ":" + string(signature)}
	}

	if debug {
		log.Printf("Signature payload: %q", payload)
		log.Printf("Signature: %q", signature)
	}
}

// amazonEscape does uri escaping exactly as Amazon does
func amazonEscape(s string) string {
	hexCount := 0

	for i := 0; i < len(s); i++ {
		if amazonShouldEscape(s[i]) {
			hexCount++
		}
	}

	if hexCount == 0 {
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		if c := s[i]; amazonShouldEscape(c) {
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		} else {
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

// amazonShouldEscape returns true if byte should be escaped
func amazonShouldEscape(c byte) bool {
	return !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') || c == '_' || c == '-' || c == '~' || c == '.' || c == '/' || c == ':')
}
