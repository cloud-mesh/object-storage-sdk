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

func GetObjectWithURL(signedURL string, timeout time.Duration) (io.ReadCloser, *http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, signedURL, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := (&http.Client{
		Timeout: timeout,
	}).Do(request)
	if err != nil {
		return nil, nil, err
	}
	if err := CheckResponse(resp); err != nil {
		return nil, nil, err
	}

	return resp.Body, resp, nil
}

func PutObjectWithURL(signedURL string, reader io.Reader, timeout time.Duration) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodPut, signedURL, reader)
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

	return resp, err
}

func FGetObjectWithURL(signedURL, localFilePath string, timeout time.Duration) error {
	reader, _, err := GetObjectWithURL(signedURL, timeout)
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

	_, err = PutObjectWithURL(signedURL, fd, timeout)
	return err
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

func CheckResponse(resp *http.Response) error {
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusMultipleChoices {
		return errors.New(fmt.Sprintf("code=%d", resp.StatusCode))
	}

	return nil
}