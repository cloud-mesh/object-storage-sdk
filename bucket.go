package object_storage_sdk

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

var BucketNotExist = errors.New("bucket doesn't exist")

type ObjectMeta struct {
	ContentType   string
	ContentLength int
	ETag          string
	LastModified  time.Time
}

type ObjectProperty struct {
	ObjectMeta
	ObjectKey string
}

func HeaderToObjectMeta(header http.Header) (objectMeta ObjectMeta, err error) {
	size, err := strconv.Atoi(header.Get("Content-Length"))
	if err != nil {
		return
	}
	lastModified, err := time.Parse(time.RFC1123, header.Get("Last-Modified"))

	return ObjectMeta{
		ContentType:   header.Get("Content-Type"),
		ContentLength: size,
		ETag:          header.Get("Etag"),
		LastModified:  lastModified,
	}, nil
}

type BasicBucket interface {
	GetObject(objectKey string) (io.ReadCloser, error)
	HeadObject(objectKey string) (object ObjectMeta, err error)
	ListObjects(objectPrefix string) (objects []ObjectProperty, err error)
	PutObject(objectKey string, reader io.ReadSeeker) error
	CopyObject(srcObjectKey, dstObjectKey string) error
	RemoveObject(objectKey string) error
	RemoveObjects(objectKeys []string) error
}

type PresignAbleBucket interface {
	PresignGetObject(objectKey string, expiresIn time.Duration) (signedURL string, err error)
	PresignHeadObject(objectKey string, expiresIn time.Duration) (signedURL string, err error)
	PresignPutObject(objectKey string, expiresIn time.Duration) (signedURL string, err error)
}

type PresignPostAbleBucket interface {
	PresignPostObject(objectKey string, expiresIn time.Duration) (signedURL, fileField string, formData map[string]string, err error)
}

type Part struct {
	PartNumber   int
	Size         int
	ETag         string
	LastModified time.Time
}

type CompletePart struct {
	PartNumber int
	ETag       string
}

type Upload struct {
	ObjectKey string
	UploadId  string
	Initiated time.Time
}

type MultipartUploadAbleBucket interface {
	ListMultipartUploads(objectKeyPrefix string) (uploads []Upload, err error)
	InitMultipartUpload(objectKey string) (uploadId string, err error)
	UploadPart(objectKey, uploadId string, partNum int, reader io.ReadSeeker) (eTag string, err error)
	ListParts(objectKey string, uploadId string) (parts []Part, err error)
	CompleteUploadPart(objectKey string, uploadId string, parts []CompletePart) error
	AbortMultipartUpload(objectKey string, uploadId string) error
}

type MultipartUploadPresignAbleBucket interface {
	PresignUploadPart(objectKey string, uploadId string, partNum int, expiresIn time.Duration) (string, error)
}
