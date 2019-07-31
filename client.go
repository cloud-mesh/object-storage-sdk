package object_storage_sdk

import (
	"time"
)

type Option struct{}

type BucketProperties struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type BasicClient interface {
	Bucket(bucketName string) (bucket BasicBucket, err error)
	MakeBucket(bucketName string, options ...Option) error
	ListBucket(options ...Option) (buckets []BucketProperties, err error)
	RemoveBucket(bucketName string) error
	CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error
}

type PolicyClient interface {
	GetBucketPolicy(bucketName string) (policy string, err error)
	SetBucketPolicy(bucketName, policy string) error
}
