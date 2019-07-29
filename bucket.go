package object_storage_sdk

import (
	"context"
	"io"
	"time"
)

type Option struct{}

type ObjectProperties struct {
	Key          string
	Type         string
	Size         int64
	ETag         string
	LastModified time.Time
}

type policyCondition struct {
	matchType string
	condition string
	value     string
}

type PostPolicy struct {
	expiration time.Time

	conditions []policyCondition

	contentLengthRange struct {
		min int64
		max int64
	}

	formData map[string]string
}

type Bucket interface {
	GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error)
	FGetObject(ctx context.Context, objectKey string, localPath string) error
	StatObject(ctx context.Context, objectKey string) (object ObjectProperties, err error)
	ListObjects(ctx context.Context, objectPrefix string) (objects []ObjectProperties, err error)
	PutObject(ctx context.Context, objectKey string, reader io.Reader) error
	FPutObject(ctx context.Context, objectKey string, filePath string) error
	CopyObject(ctx context.Context, srcObjectKey, dstObjectKey string) error
	RemoveObject(ctx context.Context, objectKey string) error
	RemoveObjects(ctx context.Context, objectKeys []string) error
	PresignGetObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error)
	PresignHeadObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error)
	PresignPutObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error)
	PresignPostObject(ctx context.Context, p *PostPolicy) (signedURL string, formData map[string]string, err error)
}
