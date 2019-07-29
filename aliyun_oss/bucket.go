package aliyun_oss

import (
	"context"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/inspii/object_storage_sdk"
	"io"
)

type ossBucket struct {
	bucket *oss.Bucket
}

func newOssBucket(client *oss.Client, bucketName string) (*ossBucket, error) {
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	return &ossBucket{bucket}, nil
}

func (c *ossBucket) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	return c.bucket.GetObject(objectKey)
}

func (c *ossBucket) FGetObject(ctx context.Context, objectKey string, localPath string) error {
	return c.bucket.GetObjectToFile(objectKey, localPath)
}

func (c *ossBucket) StatObject(ctx context.Context, objectKey string) (object sdk.ObjectProperties, err error) {
	meta, err := c.bucket.GetObjectMeta(objectKey)
	if err != nil {
		return
	}

	panic(meta) // todo
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
			Size:         object.Size,
			ETag:         object.ETag,
			LastModified: object.LastModified,
		})
	}

	return
}

func (c *ossBucket) PutObject(ctx context.Context, objectKey string, reader io.Reader) error {
	return c.bucket.PutObject(objectKey, reader)
}

func (c *ossBucket) FPutObject(ctx context.Context, objectKey string, filePath string) error {
	return c.bucket.PutObjectFromFile(objectKey, filePath)
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

func (c *ossBucket) PresignGetObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPGet, expiredInSec)
}

func (c *ossBucket) PresignHeadObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPHead, expiredInSec)
}

func (c *ossBucket) PresignPutObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error) {
	return c.bucket.SignURL(objectKey, oss.HTTPPut, expiredInSec)
}

func (c *ossBucket) PresignPostObject(ctx context.Context, p *sdk.PostPolicy) (signedURL string, formData map[string]string, err error) {
	panic("not implemented")
}
