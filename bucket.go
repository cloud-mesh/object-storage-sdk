package object_storage_sdk

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const tempFileSuffix = ".temp"
const filePermMode = os.FileMode(0664) // Default file permission

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
	StatObject(objectKey string) (object ObjectMeta, err error)
	ListObjects(objectPrefix string) (objects []ObjectProperty, err error)
	PutObject(objectKey string, reader io.Reader) error
	CopyObject(srcObjectKey, dstObjectKey string) error
	RemoveObject(objectKey string) error
	RemoveObjects(objectKeys []string) error
}

type PresignBucket interface {
	PresignGetObject(objectKey string, expiresIn time.Duration) (signedURL string, err error)
	PresignHeadObject(objectKey string, expiresIn time.Duration) (signedURL string, err error)
	PresignPutObject(objectKey string, expiresIn time.Duration) (signedURL string, err error)
}

func FGetObject(bucket BasicBucket, objectKey string, localFilePath string) error {
	body, err := bucket.GetObject(objectKey)
	if err != nil {
		return err
	}
	defer body.Close()

	return fGet(body, localFilePath)
}

func FPutObject(ctx context.Context, bucket BasicBucket, objectKey string, localFilePath string) error {
	fd, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	return bucket.PutObject(objectKey, fd)
}

func HeadObjectWithURL(signedURL string, timeout time.Duration) (http.Header, error) {
	request, err := http.NewRequest(http.MethodHead, signedURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{
		Timeout: timeout,
	}).Do(request)
	if err != nil {
		return nil, err
	}
	if err := CheckResponse(resp); err != nil {
		return nil, err
	}

	return resp.Header, nil
}

func GetObjectWithURL(signedURL string, timeout time.Duration) (io.ReadCloser, error) {
	request, err := http.NewRequest(http.MethodGet, signedURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{
		Timeout: timeout,
	}).Do(request)
	if err != nil {
		return nil, err
	}
	if err := CheckResponse(resp); err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func PutObjectWithURL(signedURL string, reader io.Reader, timeout time.Duration) error {
	request, err := http.NewRequest(http.MethodPut, signedURL, reader)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{
		Timeout: timeout,
	}).Do(request)
	if err != nil {
		return err
	}
	if err := CheckResponse(resp); err != nil {
		return err
	}

	return err
}

func FGetObjectWithURL(signedURL, localFilePath string, timeout time.Duration) error {
	reader, err := GetObjectWithURL(signedURL, timeout)
	defer reader.Close()
	if err != nil {
		return err
	}
	return fGet(reader, localFilePath)
}

func FPutObjectWithURL(signedURL, localFilePath string, timeout time.Duration) error {
	fd, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	return PutObjectWithURL(signedURL, fd, timeout)
}

func fGet(reader io.Reader, localFilePath string) error {
	tempFilePath := localFilePath + tempFileSuffix
	fd, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filePermMode)
	if err != nil {
		return err
	}
	defer fd.Close()

	if err != nil {
		return err
	}

	_, err = io.Copy(fd, reader)
	if err != nil {
		return err
	}

	return os.Rename(tempFilePath, localFilePath)
}
