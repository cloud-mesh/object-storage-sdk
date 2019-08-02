package huaweicloud_obs

import (
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/impl/huaweicloud_obs/obs"
)

func NewClient(location string, client *obs.ObsClient) *obsClient {
	return &obsClient{location: location, client: client}
}

type obsClient struct {
	location string
	client   *obs.ObsClient
}

func (c *obsClient) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	return newObsBucket(bucketName, c.client)
}

func (c *obsClient) HeadBucket(bucketName string) error {
	_, err := c.client.HeadBucket(bucketName)
	return err
}

func (c *obsClient) MakeBucket(bucketName string, options ...sdk.Option) error {
	input := &obs.CreateBucketInput{
		BucketLocation: obs.BucketLocation{
			Location: c.location,
		},
		Bucket: bucketName,
	}

	_, err := c.client.CreateBucket(input)
	return err
}

func (c *obsClient) ListBucket(options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
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

func (c *obsClient) RemoveBucket(bucketName string) error {
	_, err := c.client.DeleteBucket(bucketName)
	return err
}

func (c *obsClient) CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
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

func (c *obsClient) GetBucketPolicy(bucketName string) (policy string, err error) {
	result, err := c.client.GetBucketPolicy(bucketName)
	if err != nil {
		return
	}
	return result.Policy, nil
}

func (c *obsClient) SetBucketPolicy(bucketName, policy string) error {
	input := &obs.SetBucketPolicyInput{
		Bucket: bucketName,
		Policy: policy,
	}
	_, err := c.client.SetBucketPolicy(input)
	return err
}
