package huaweicloud_obs

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/huaweicloud_obs/obs"
)

func NewClient(ak, sk, endpoint string) (client *obsClient, err error) {
	c, err := obs.New(ak, sk, endpoint)
	if err != nil {
		return
	}

	return &obsClient{c}, nil
}

type obsClient struct {
	client *obs.ObsClient
}

func (c *obsClient) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	return newObsBucket(bucketName, c.client)
}

func (c *obsClient) MakeBucket(ctx context.Context, bucketName string, options ...sdk.Option) error {
	input := &obs.CreateBucketInput{
		Bucket: bucketName,
	}

	_, err := c.client.CreateBucket(input)
	return err
}

func (c *obsClient) ListBucket(ctx context.Context, options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
	input := &obs.ListBucketsInput{}
	result, err := c.client.ListBuckets(input)
	if err != nil {
		return
	}

	for _, bucket := range result.Buckets {
		buckets = append(buckets, sdk.BucketProperties{
			Name:      bucket.Name,
			CreatedAt: bucket.CreationDate,
		})
	}

	return
}

func (c *obsClient) RemoveBucket(ctx context.Context, bucketName string) error {
	_, err := c.client.DeleteBucket(bucketName)
	return err
}

func (c *obsClient) CopyObject(ctx context.Context, srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	input := &obs.CopyObjectInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: dstBucketName,
			Key:    dstObjectKey,
		},
		CopySourceBucket: srcBucketName,
		CopySourceKey:    srcObjectKey,
	}
	_, err := c.client.CopyObject(input)
	return err
}

func (c *obsClient) GetBucketPolicy(ctx context.Context, bucketName string) (policy string, err error) {
	result, err := c.client.GetBucketPolicy(bucketName)
	if err != nil {
		return
	}
	return result.Policy, nil
}

func (c *obsClient) SetBucketPolicy(ctx context.Context, bucketName, policy string) error {
	input := &obs.SetBucketPolicyInput{
		Bucket: bucketName,
		Policy: policy,
	}
	_, err := c.client.SetBucketPolicy(input)
	return err
}
