package aws_s3

import (
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/minio/minio-go"
)

func NewClient(region, endpoint string, accessKeyID, secretAccessKey string, options ...option) (*minioClient, error) {
	config := config{}
	for _, option := range options {
		option(&config)
	}

	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, config.useSSL)
	if err != nil {
		return nil, err
	}

	return &minioClient{
		region: region,
		client: client,
		config: config,
	}, nil
}

type minioClient struct {
	region string
	client *minio.Client
	config config
}

func (c *minioClient) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	return newMinioBucket(bucketName, c), nil
}

func (c *minioClient) MakeBucket(bucketName string, options ...sdk.Option) error {
	return c.client.MakeBucket(bucketName, c.region)
}

func (c *minioClient) ListBucket(options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
	list, err := c.client.ListBuckets()
	if err != nil {
		return
	}

	for _, bucket := range list {
		buckets = append(buckets, sdk.BucketProperties{
			Name:      bucket.Name,
			CreatedAt: bucket.CreationDate,
		})
	}

	return
}

func (c *minioClient) RemoveBucket(bucketName string) error {
	return c.client.RemoveBucket(bucketName)
}

func (c *minioClient) CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	dst, err := minio.NewDestinationInfo(dstBucketName, dstObjectKey, nil, nil)
	if err != nil {
		return err
	}
	src := minio.NewSourceInfo(srcBucketName, srcObjectKey, nil)
	return c.client.CopyObject(dst, src)
}
