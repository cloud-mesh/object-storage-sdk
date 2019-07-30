package object_storage_sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const tempFileSuffix = ".temp"
const filePermMode = os.FileMode(0664) // Default file permission

type ObjectProperties struct {
	Key          string
	Type         string
	Size         int
	ETag         string
	LastModified time.Time
}

type BasicBucket interface {
	GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error)
	StatObject(ctx context.Context, objectKey string) (object ObjectProperties, err error)
	ListObjects(ctx context.Context, objectPrefix string) (objects []ObjectProperties, err error)
	PutObject(ctx context.Context, objectKey string, reader io.Reader) error
	CopyObject(ctx context.Context, srcObjectKey, dstObjectKey string) error
	RemoveObject(ctx context.Context, objectKey string) error
	RemoveObjects(ctx context.Context, objectKeys []string) error
}

type PresignBucket interface {
	PresignGetObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error)
	PresignHeadObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error)
	PresignPutObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error)
}

type PresignFormBucket interface {
	PresignPostObject(ctx context.Context, objectKey string, expiresIn time.Duration) (postURL string, formData map[string]string, err error)
	FPostFormObject(postURL string, formData map[string]string, localFilePath string, timeout time.Duration) error
}

func FGetObject(ctx context.Context, bucket BasicBucket, objectKey string, localFilePath string) error {
	body, err := bucket.GetObject(ctx, objectKey)
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

	return bucket.PutObject(ctx, objectKey, fd)
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
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusMultipleChoices {
		return nil, errors.New(fmt.Sprintf("code=%d", resp.StatusCode))
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
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusMultipleChoices {
		return errors.New(fmt.Sprintf("code=%d", resp.StatusCode))
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
