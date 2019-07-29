package aliyun_oss

import (
	"context"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/inspii/object_storage_sdk"
)

func NewClient(endpoint, accessKey, accessId string) (client *ossClient, err error) {
	c, err := oss.New(endpoint, accessKey, accessId)
	if err != nil {
		return
	}

	return &ossClient{c}, nil
}

type ossClient struct {
	client *oss.Client
}

func (c *ossClient) Bucket(bucketName string) (sdk.Bucket, error) {
	return newOssBucket(c.client, bucketName)
}

func (c *ossClient) MakeBucket(ctx context.Context, bucketName string, options ...sdk.Option) error {
	return c.client.CreateBucket(bucketName)
}

func (c *ossClient) ListBucket(ctx context.Context, options ...sdk.Option) ([]sdk.BucketProperties, error) {
	list, err := c.client.ListBuckets()
	if err != nil {
		return nil, err
	}

	buckets := make([]sdk.BucketProperties, 0, len(list.Buckets))
	for _, bucket := range list.Buckets {
		buckets = append(buckets, sdk.BucketProperties{
			Name:      bucket.Name,
			CreatedAt: bucket.CreationDate,
		})
	}

	return buckets, nil
}

func (c *ossClient) RemoveBucket(ctx context.Context, bucketName string) error {
	return c.client.DeleteBucket(bucketName)
}

func (c *ossClient) CopyObject(ctx context.Context, srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	bucket, err := c.client.Bucket(srcBucketName)
	if err != nil {
		return err
	}

	_, err = bucket.CopyObjectTo(dstBucketName, dstObjectKey, srcObjectKey)
	return err
}

func (c *ossClient) GetBucketPolicy(ctx context.Context, bucketName string) (policy string, err error) {
	return c.client.GetBucketPolicy(bucketName)
}

func (c *ossClient) SetBucketPolicy(ctx context.Context, bucketName, policy string) error {
	return c.client.SetBucketPolicy(bucketName, policy)
}
