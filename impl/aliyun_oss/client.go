package aliyun_oss

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/inspii/object_storage_sdk"
)

func NewClient(endpoint, accessKeyId, accessKeySecret string, options ...oss.ClientOption) (client *ossClient, err error) {
	c, err := oss.New(endpoint, accessKeyId, accessKeySecret, options...)
	if err != nil {
		return
	}

	return &ossClient{endpoint, c}, nil
}

type ossClient struct {
	endpoint string
	client   *oss.Client
}

func (c *ossClient) Bucket(bucketName string) (sdk.BasicBucket, error) {
	return newOssBucket(bucketName, c)
}

func (c *ossClient) HeadBucket(bucketName string) error {
	exist, err := c.client.IsBucketExist(bucketName)
	if err != nil {
		return err
	}
	if !exist {
		return sdk.BucketNotExist
	}

	return nil
}

func (c *ossClient) GetBucketLocation(bucketName string) (location string, err error) {
	return c.client.GetBucketLocation(bucketName)
}

func (c *ossClient) MakeBucket(bucketName string, options ...sdk.Option) error {
	var ossOptions []oss.Option
	config := sdk.GetConfig(options...)
	if config.ACLType != "" {
		ossOptions = append(ossOptions, oss.ACL(ossAcl(config.ACLType)))
	}

	return c.client.CreateBucket(bucketName, ossOptions...)
}

func (c *ossClient) ListBucket(options ...sdk.Option) ([]sdk.BucketProperties, error) {
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

func (c *ossClient) RemoveBucket(bucketName string) error {
	return c.client.DeleteBucket(bucketName)
}

func (c *ossClient) CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	bucket, err := c.client.Bucket(srcBucketName)
	if err != nil {
		return err
	}

	_, err = bucket.CopyObjectTo(dstBucketName, dstObjectKey, srcObjectKey)
	return err
}
