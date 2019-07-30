package object_storage_sdk

import (
	"context"
	"time"
)

type Option struct{}

type BucketProperties struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type BasicClient interface {
	Bucket(bucketName string) (bucket BasicBucket, err error)
	MakeBucket(ctx context.Context, bucketName string, options ...Option) error
	ListBucket(ctx context.Context, options ...Option) (buckets []BucketProperties, err error)
	RemoveBucket(ctx context.Context, bucketName string) error
	CopyObject(ctx context.Context, srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error
}

type PolicyClient interface {
	GetBucketPolicy(ctx context.Context, bucketName string) (policy string, err error)
	SetBucketPolicy(ctx context.Context, bucketName, policy string) error
}
