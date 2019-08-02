package aliyun_oss

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/inspii/object_storage_sdk"
	"io"
	"time"
)

type ossBucket struct {
	client *ossClient
	bucket *oss.Bucket
}

func newOssBucket(bucketName string, client *ossClient) (*ossBucket, error) {
	bucket, err := client.client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	return &ossBucket{client, bucket}, nil
}

func (c *ossBucket) GetObject(objectKey string) (io.ReadCloser, error) {
	return c.bucket.GetObject(objectKey)
}

func (c *ossBucket) HeadObject(objectKey string) (object sdk.ObjectMeta, err error) {
	header, err := c.bucket.GetObjectMeta(objectKey)
	if err != nil {
		return
	}

	return sdk.HeaderToObjectMeta(header)
}

func (c *ossBucket) ListObjects(objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	result, err := c.bucket.ListObjects()
	if err != nil {
		return
	}

	for _, object := range result.Objects {
		objects = append(objects, sdk.ObjectProperty{
			ObjectKey: object.Key,
			ObjectMeta: sdk.ObjectMeta{
				ContentType:   object.Type,
				ContentLength: int(object.Size),
				ETag:          object.ETag,
				LastModified:  object.LastModified,
			},
		})
	}

	return
}

func (c *ossBucket) PutObject(objectKey string, reader io.ReadSeeker) error {
	return c.bucket.PutObject(objectKey, reader)
}

func (c *ossBucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	_, err := c.bucket.CopyObject(srcObjectKey, dstObjectKey)
	return err
}

func (c *ossBucket) RemoveObject(objectKey string) error {
	err := c.bucket.DeleteObject(objectKey)
	return err
}

func (c *ossBucket) RemoveObjects(objectKeys []string) error {
	_, err := c.bucket.DeleteObjects(objectKeys)
	return err
}

func (c *ossBucket) PresignGetObject(objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPGet, int64(expiresIn/time.Second))
}

func (c *ossBucket) PresignHeadObject(objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPHead, int64(expiresIn/time.Second))
}

func (c *ossBucket) PresignPutObject(objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPPut, int64(expiresIn/time.Second))
}
