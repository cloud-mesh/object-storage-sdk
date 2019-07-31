package tencent_cos

import (
	"context"
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/url"
	"time"
)

func NewClient(region, secretId, secretKey string) *cosClient {
	return &cosClient{
		region:    region,
		secretId:  secretId,
		secretKey: secretKey,
	}
}

type cosClient struct {
	region    string
	secretId  string
	secretKey string
}

func (c *cosClient) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	client, err := c.bucketClient(bucketName)
	if err != nil {
		return nil, err
	}

	return newBucket(c.region, bucketName, client), nil
}

func (c *cosClient) MakeBucket(ctx context.Context, bucketName string, options ...sdk.Option) error {
	client, err := c.bucketClient(bucketName)
	if err != nil {
		return err
	}

	_, err = client.Bucket.Put(ctx, nil)
	return err
}

func (c *cosClient) ListBucket(ctx context.Context, options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
	client := cos.NewClient(nil, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.secretId,
			SecretKey: c.secretKey,
		},
	})
	result, _, err := client.Service.Get(ctx)
	if err != nil {
		return nil, err
	}

	for _, bucket := range result.Buckets {
		createdAt, err := time.Parse(time.RFC1123, bucket.CreationDate)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, sdk.BucketProperties{
			Name:      bucket.Name,
			CreatedAt: createdAt,
		})
	}

	return
}

func (c *cosClient) RemoveBucket(ctx context.Context, bucketName string) error {
	client, err := c.bucketClient(bucketName)
	if err != nil {
		return err
	}

	_, err = client.Bucket.Delete(ctx)
	return err
}

func (c *cosClient) CopyObject(ctx context.Context, srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	client, err := c.bucketClient(dstBucketName)
	if err != nil {
		return err
	}

	_, _, err = client.Object.Copy(ctx, dstObjectKey, objectURL(c.region, srcBucketName, srcObjectKey), nil)
	return err
}

func (c *cosClient) bucketClient(bucketName string) (*cos.Client, error) {
	u, _ := url.Parse(bucketURL(c.region, bucketName))
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.secretId,
			SecretKey: c.secretKey,
		},
	})

	_, err := client.Bucket.Put(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func bucketURL(region, bucketName string) string {
	return cos.NewBucketURL(bucketName, region, false).String()
}

func objectURL(region, bucketName, objectKey string) string {
	return fmt.Sprintf("%s/%s", bucketURL(region, bucketName), objectKey)
}
