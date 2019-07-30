package qiniu_kodo

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/qiniu/api.v7/auth/qbox"
	"github.com/qiniu/api.v7/storage"
)

func NewClient(accessKey, secretKey string) *kodoClient {
	mac := qbox.NewMac(accessKey, secretKey)
	cfg := storage.Config{
		UseHTTPS: false,
	}

	bucketManager := storage.NewBucketManager(mac, &cfg)
	return &kodoClient{bucketManager: bucketManager}
}

type kodoClient struct {
	bucketManager *storage.BucketManager
}

func (c *kodoClient) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	return newKodoBucket(bucketName, c.bucketManager), nil
}

func (c *kodoClient) MakeBucket(ctx context.Context, bucketName string, options ...sdk.Option) error {
	panic("implement me")
}

func (c *kodoClient) ListBucket(ctx context.Context, options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
	bucketNames, err := c.bucketManager.Buckets(true)
	if err != nil {
		return
	}
	for _, bucketName := range bucketNames {
		buckets = append(buckets, sdk.BucketProperties{
			Name: bucketName,
		})
	}

	return
}

func (c *kodoClient) RemoveBucket(ctx context.Context, bucketName string) error {
	panic("implement me")
}

func (c *kodoClient) CopyObject(ctx context.Context, srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	return c.bucketManager.Copy(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey, true)
}
