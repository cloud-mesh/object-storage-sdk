package aws_s3

import (
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/minio/minio-go"
	"io"
	"time"
)

func newMinioBucket(bucketName string, client *minioClient) *minioBucket {
	return &minioBucket{bucketName, client}
}

type minioBucket struct {
	bucketName string
	*minioClient
}

func (b *minioBucket) GetObject(objectKey string) (io.ReadCloser, error) {
	obj, err := b.client.GetObject(b.bucketName, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return obj, err
}

func (b *minioBucket) StatObject(objectKey string) (object sdk.ObjectMeta, err error) {
	info, err := b.client.StatObject(b.bucketName, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return
	}

	return sdk.ObjectMeta{
		ContentType:   info.ContentType,
		ContentLength: int(info.Size),
		ETag:          info.ETag,
		LastModified:  info.LastModified,
	}, nil
}

func (b *minioBucket) ListObjects(objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	doneCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(doneCh)
	}()

	objectsChan := b.client.ListObjects(b.bucketName, objectPrefix, true, doneCh)
	for object := range objectsChan {
		objects = append(objects, sdk.ObjectProperty{
			ObjectKey: object.Key,
			ObjectMeta: sdk.ObjectMeta{
				ContentType:   object.ContentType,
				ContentLength: int(object.Size),
				ETag:          object.ETag,
				LastModified:  object.LastModified,
			},
		})
	}

	return
}

func (b *minioBucket) PutObject(objectKey string, reader io.Reader, objectSize int) error {
	_, err := b.client.PutObject(b.bucketName, objectKey, reader, int64(objectSize), minio.PutObjectOptions{})
	return err
}

func (b *minioBucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	dst, err := minio.NewDestinationInfo(b.bucketName, dstObjectKey, nil, nil)
	if err != nil {
		return err
	}
	src := minio.NewSourceInfo(b.bucketName, srcObjectKey, nil)
	return b.client.CopyObject(dst, src)
}

func (b *minioBucket) RemoveObject(objectKey string) error {
	return b.client.RemoveObject(b.bucketName, objectKey)
}

func (b *minioBucket) RemoveObjects(objectKeys []string) error {
	objectsCh := make(chan string, 1)
	go func() {
		for _, objectKey := range objectKeys {
			objectsCh <- objectKey
		}
		close(objectsCh)
	}()

	errorCh := b.client.RemoveObjects(b.bucketName, objectsCh)
	for err := range errorCh {
		return err.Err
	}

	return nil
}

func (b *minioBucket) PresignGetObject(objectKey string, expiresIn time.Duration) (string, error) {
	url, err := b.client.PresignedGetObject(b.bucketName, objectKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (b *minioBucket) PresignHeadObject(objectKey string, expiresIn time.Duration) (string, error) {
	url, err := b.client.PresignedHeadObject(b.bucketName, objectKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (b *minioBucket) PresignPutObject(objectKey string, expiresIn time.Duration) (string, error) {
	url, err := b.client.PresignedPutObject(b.bucketName, objectKey, expiresIn)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}
