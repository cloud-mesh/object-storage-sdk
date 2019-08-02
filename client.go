package object_storage_sdk

import (
	"time"
)

type BucketProperties struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type BasicClient interface {
	Bucket(bucketName string) (bucket BasicBucket, err error)
	MakeBucket(bucketName string, options ...Option) error
	HeadBucket(bucketName string) error
	GetBucketACL(bucketName string) (acl ACLType, err error)
	PutBucketACL(bucketName string, acl ACLType) error
	GetBucketLocation(bucketName string) (location string, err error)
	ListBucket(options ...Option) (buckets []BucketProperties, err error)
	RemoveBucket(bucketName string) error
	CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error
}