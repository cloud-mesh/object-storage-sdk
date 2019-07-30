package aliyun_oss

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/inspii/object_storage_sdk"
	"hash"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	presignPostGmtIso8601 = "2006-01-02T15:04:05Z"
	presignPostParamOrder = "name,key,policy,OSSAccessKeyId,success_action_status,callback,signature"
	presignPostFileFiled  = "file"
)

type ossBucket struct {
	client *ossClient
	bucket *oss.Bucket
}

func newOssBucket(client *ossClient, bucketName string) (*ossBucket, error) {
	bucket, err := client.client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	return &ossBucket{client, bucket}, nil
}

func (c *ossBucket) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	return c.bucket.GetObject(objectKey)
}

func (c *ossBucket) StatObject(ctx context.Context, objectKey string) (object sdk.ObjectProperties, err error) {
	meta, err := c.bucket.GetObjectMeta(objectKey)
	if err != nil {
		return
	}

	size, err := strconv.Atoi(meta.Get("Content-Length"))
	if err != nil {
		return
	}
	lastModified, err := time.Parse(time.RFC1123, meta.Get("Last-Modified"))

	object = sdk.ObjectProperties{
		Key:          objectKey,
		Type:         "",
		Size:         size,
		ETag:         meta.Get("Etag"),
		LastModified: lastModified,
	}

	return
}

func (c *ossBucket) ListObjects(ctx context.Context, objectPrefix string) (objects []sdk.ObjectProperties, err error) {
	result, err := c.bucket.ListObjects()
	if err != nil {
		return
	}

	for _, object := range result.Objects {
		objects = append(objects, sdk.ObjectProperties{
			Key:          object.Key,
			Type:         object.Type,
			Size:         int(object.Size),
			ETag:         object.ETag,
			LastModified: object.LastModified,
		})
	}

	return
}

func (c *ossBucket) PutObject(ctx context.Context, objectKey string, reader io.Reader) error {
	return c.bucket.PutObject(objectKey, reader)
}

func (c *ossBucket) CopyObject(ctx context.Context, srcObjectKey, dstObjectKey string) error {
	_, err := c.bucket.CopyObject(srcObjectKey, dstObjectKey)
	return err
}

func (c *ossBucket) RemoveObject(ctx context.Context, objectKey string) error {
	err := c.bucket.DeleteObject(objectKey)
	return err
}

func (c *ossBucket) RemoveObjects(ctx context.Context, objectKeys []string) error {
	_, err := c.bucket.DeleteObjects(objectKeys)
	return err
}

func (c *ossBucket) PresignGetObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPGet, int64(expiresIn/time.Second))
}

func (c *ossBucket) PresignHeadObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPHead, int64(expiresIn/time.Second))
}

func (c *ossBucket) PresignPutObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPPut, int64(expiresIn/time.Second))
}

// https://help.aliyun.com/document_detail/31927.html?spm=a2c4g.11186623.6.1331.616c65d3fpq3da
func (c *ossBucket) PresignPostObject(ctx context.Context, objectKey string, expiresIn time.Duration) (postURL string, formData map[string]string, err error) {
	expireAt := time.Now().Add(expiresIn)
	config := struct {
		Expiration string     `json:"expiration"`
		Conditions [][]string `json:"conditions"`
	}{
		Expiration: expireAt.Format(presignPostGmtIso8601),
		Conditions: [][]string{{"eq", "$key", objectKey}},
	}
	configStr, err := json.Marshal(config)
	if err != nil {
		return
	}
	configBase64 := base64.StdEncoding.EncodeToString(configStr)
	h := hmac.New(func() hash.Hash { return sha1.New() }, []byte(c.client.accessKeySecret))
	io.WriteString(h, configBase64)
	signedStr := base64.StdEncoding.EncodeToString(h.Sum(nil))

	callbackParam := struct {
		CallbackUrl      string `json:"callbackUrl"`
		CallbackBody     string `json:"callbackBody"`
		CallbackBodyType string `json:"callbackBodyType"`
	}{
		CallbackUrl:      "",
		CallbackBody:     "filename=${object}&size=${size}&mimeType=${mimeType}&height=${imageInfo.height}&width=${imageInfo.width}",
		CallbackBodyType: "application/x-www-form-urlencoded",
	}
	callbackParamStr, err := json.Marshal(callbackParam)
	if err != nil {
		return
	}
	callbackBase64 := base64.StdEncoding.EncodeToString(callbackParamStr)
	postURL = c.getHost(c.client.endpoint, c.bucket.BucketName)

	formData = map[string]string{
		"name":                  filepath.Base(objectKey),
		"key":                   objectKey,
		"policy":                string(configBase64),
		"OSSAccessKeyId":        c.client.accessKeyId,
		"success_action_status": "200",
		"callback":              callbackBase64,
		"signature":             string(signedStr),
	}
	return
}

func (c *ossBucket) FPostFormObject(postURL string, formData map[string]string, localFilePath string, timeout time.Duration) error {
	bodyBuf := new(bytes.Buffer)
	bodyWriter := multipart.NewWriter(bodyBuf)

	formParamOrder := strings.Split(",", presignPostParamOrder)
	for _, key := range formParamOrder {
		if err := bodyWriter.WriteField(key, formData[key]); err != nil {
			return err
		}
	}

	file, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	fileWriter, err := bodyWriter.CreateFormFile(presignPostFileFiled, filepath.Base(localFilePath))
	if err != nil {
		return err
	}
	_, err = io.Copy(fileWriter, file)
	if err != nil {
		return err
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()
	request, err := http.NewRequest(http.MethodPost, postURL, bodyBuf)
	if err != nil {
		return err
	}
	request.Header["Content-Type"] = []string{contentType}

	resp, err := (&http.Client{Timeout: timeout}).Do(request)
	if err != nil {
		return err
	}

	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusMultipleChoices {
		return errors.New(fmt.Sprintf("code=%d", resp.StatusCode))
	}

	return nil
}

func (c *ossBucket) getObjectURL(object string) string {
	host := c.getHost(c.client.endpoint, c.bucket.BucketName)
	return fmt.Sprintf("%s/%s", host, object)
}

func (c *ossBucket) getHost(endpoint, bucket string) string {
	return fmt.Sprintf("http://%s.%s", bucket, endpoint)
}
