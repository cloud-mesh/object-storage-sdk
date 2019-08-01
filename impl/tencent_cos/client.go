package tencent_cos

import (
	"context"
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const gmtIso8601 = "2006-01-02T15:04:05Z"

func NewClient(region, appId, secretId, secretKey string) *cosClient {
	return &cosClient{
		region:    region,
		appId:     appId,
		secretId:  secretId,
		secretKey: secretKey,
	}
}

type cosClient struct {
	region    string
	appId     string
	secretId  string
	secretKey string
}

func (c *cosClient) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	return newBucket(bucketName, c)
}

func (c *cosClient) MakeBucket(bucketName string, options ...sdk.Option) error {
	client, err := c.bucketClient(bucketName)
	if err != nil {
		return err
	}

	_, err = client.Bucket.Put(context.TODO(), nil)
	return err
}

func (c *cosClient) ListBucket(options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
	client := cos.NewClient(nil, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.secretId,
			SecretKey: c.secretKey,
		},
	})
	result, _, err := client.Service.Get(context.TODO())
	if err != nil {
		return nil, err
	}

	for _, bucket := range result.Buckets {
		createdAt, err := time.Parse(gmtIso8601, bucket.CreationDate)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, sdk.BucketProperties{
			Name:      bucketNameWithoutAPPID(bucket.Name, c.appId),
			CreatedAt: createdAt,
		})
	}

	return
}

func (c *cosClient) RemoveBucket(bucketName string) error {
	client, err := c.bucketClient(bucketName)
	if err != nil {
		return err
	}

	_, err = client.Bucket.Delete(context.TODO())
	return err
}

func (c *cosClient) CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	client, err := c.bucketClient(dstBucketName)
	if err != nil {
		return err
	}

	_, _, err = client.Object.Copy(context.TODO(), dstObjectKey, objectURI(c.region, c.appId, srcBucketName, srcObjectKey), nil)
	return err
}

func (c *cosClient) bucketClient(bucketName string) (*cos.Client, error) {
	u, _ := url.Parse(bucketURL(c.region, c.appId, bucketName))
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.secretId,
			SecretKey: c.secretKey,
		},
	})

	return client, nil
}

func bucketNameWithoutAPPID(bucketNameWithAppID, appID string) string {
	index := strings.LastIndex(bucketNameWithAppID, "-"+appID)
	if index < 0 {
		return bucketNameWithAppID
	}

	return bucketNameWithAppID[0:index]
}

func bucketURL(region, appId, bucketName string) string {
	bucketName = fmt.Sprintf("%s-%s", bucketName, appId)
	return cos.NewBucketURL(bucketName, region, true).String()
}

func bucketHost(region, appId, bucketName string) string {
	bucketName = fmt.Sprintf("%s-%s", bucketName, appId)
	return cos.NewBucketURL(bucketName, region, true).Host
}

func objectURI(region, appId, bucketName, objectKey string) string {
	return fmt.Sprintf("%s/%s", bucketHost(region, appId, bucketName), objectKey)
}
