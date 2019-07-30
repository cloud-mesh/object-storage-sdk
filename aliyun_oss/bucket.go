package aliyun_oss

import (
	"context"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/inspii/object_storage_sdk"
	"io"
	"strconv"
	"time"
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

func (c *ossBucket) StatObject(ctx context.Context, objectKey string) (object sdk.ObjectMeta, err error) {
	header, err := c.bucket.GetObjectMeta(objectKey)
	if err != nil {
		return
	}

	size, err := strconv.Atoi(header.Get("Content-Length"))
	if err != nil {
		return
	}
	lastModified, err := time.Parse(time.RFC1123, header.Get("Last-Modified"))

	object = sdk.ObjectMeta{
		ContentType:   header.Get("Content-Type"),
		ContentLength: size,
		ETag:          header.Get("Etag"),
		LastModified:  lastModified,
	}

	return
}

func (c *ossBucket) ListObjects(ctx context.Context, objectPrefix string) (objects []sdk.ObjectProperty, err error) {
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
