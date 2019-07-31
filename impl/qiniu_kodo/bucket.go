package qiniu_kodo

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/qiniu/api.v7/storage"
	"io"
	"time"
)

func newKodoBucket(bucketName string, bucketMananger *storage.BucketManager) *kodoBucket {
	return &kodoBucket{
		bucketName:    bucketName,
		bucketManager: bucketMananger,
	}
}

type kodoBucket struct {
	bucketName    string
	bucketManager *storage.BucketManager
}

func (b *kodoBucket) GetObject(objectKey string) (io.ReadCloser, error) {
	panic("implement me")
}

func (b *kodoBucket) StatObject(objectKey string) (object sdk.ObjectMeta, err error) {
	info, err := b.bucketManager.Stat(b.bucketName, objectKey)
	if err != nil {
		return
	}

	object = sdk.ObjectMeta{
		ContentType:   info.MimeType,
		ContentLength: int(info.Fsize),
		ETag:          info.Hash,
		LastModified:  time.Unix(info.PutTime, 0),
	}
	return
}

func (b *kodoBucket) ListObjects(objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	retCh, err := b.bucketManager.ListBucketContext(context.TODO(), b.bucketName, objectPrefix, "", "")
	if err != nil {
		return
	}

	for fileInfo := range retCh {
		objects = append(objects, sdk.ObjectProperty{
			ObjectKey: fileInfo.Item.Key,
			ObjectMeta: sdk.ObjectMeta{
				ContentType:   fileInfo.Item.MimeType,
				ContentLength: int(fileInfo.Item.Fsize),
				ETag:          fileInfo.Item.Hash,
				LastModified:  time.Unix(fileInfo.Item.PutTime, 0),
			},
		})
	}

	return
}

func (b *kodoBucket) PutObject(objectKey string, reader io.Reader) error {
	panic("implement me")
}

func (b *kodoBucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	return b.bucketManager.Copy(b.bucketName, srcObjectKey, b.bucketName, dstObjectKey, true)
}

func (b *kodoBucket) RemoveObject(objectKey string) error {
	return b.bucketManager.Delete(b.bucketName, objectKey)
}

func (b *kodoBucket) RemoveObjects(objectKeys []string) error {
	for _, objectKey := range objectKeys {
		if err := b.bucketManager.Delete(b.bucketName, objectKey); err != nil {
			return err
		}
	}

	return nil
}
