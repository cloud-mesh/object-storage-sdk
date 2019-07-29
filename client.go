package object_storage_sdk

import (
	"context"
	"time"
)

type BucketProperties struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type Client interface {
	Bucket(bucketName string) (bucket Bucket, err error)
	MakeBucket(ctx context.Context, bucketName string, options ...Option) error
	ListBucket(ctx context.Context, options ...Option) (buckets []BucketProperties, err error)
	RemoveBucket(ctx context.Context, bucketName string) error
	CopyObject(ctx context.Context, srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error
	GetBucketPolicy(ctx context.Context, bucketName string) (policy string, err error)
	SetBucketPolicy(ctx context.Context, bucketName, policy string) error
}
