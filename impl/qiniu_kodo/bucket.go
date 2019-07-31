package qiniu_kodo

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/qiniu/api.v7/storage"
	"io"
	"time"
)

const downloadObjectTimeout = time.Hour

func newKodoBucket(bucketName string, client *kodoClient) *kodoBucket {
	cfg := &storage.Config{
		UseHTTPS:      false,
		UseCdnDomains: false,
	}
	uploader := storage.NewFormUploader(cfg)
	return &kodoBucket{
		bucketName:     bucketName,
		client:         client,
		bucketUploader: uploader,
	}
}

type kodoBucket struct {
	bucketName     string
	client         *kodoClient
	bucketUploader *storage.FormUploader
}

func (b *kodoBucket) GetObject(objectKey string) (io.ReadCloser, error) {
	deadline := time.Now().Add(downloadObjectTimeout).Unix()
	url := storage.MakePrivateURL(b.client.bucketManager.Mac, b.client.privateDomain, objectKey, deadline)
	return sdk.GetObjectWithURL(url, downloadObjectTimeout)
}

func (b *kodoBucket) StatObject(objectKey string) (object sdk.ObjectMeta, err error) {
	info, err := b.client.bucketManager.Stat(b.bucketName, objectKey)
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
	retCh, err := b.client.bucketManager.ListBucketContext(context.TODO(), b.bucketName, objectPrefix, "", "")
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
	scope := b.bucketName + ":" + objectKey
	policy := &storage.PutPolicy{Scope: scope}
	uploadToken := policy.UploadToken(b.client.bucketManager.Mac)
	ret := storage.PutRet{}

	return b.bucketUploader.Put(context.TODO(), &ret, uploadToken, objectKey, reader, 0, nil)
}

func (b *kodoBucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	return b.client.bucketManager.Copy(b.bucketName, srcObjectKey, b.bucketName, dstObjectKey, true)
}

func (b *kodoBucket) RemoveObject(objectKey string) error {
	return b.client.bucketManager.Delete(b.bucketName, objectKey)
}

func (b *kodoBucket) RemoveObjects(objectKeys []string) error {
	for _, objectKey := range objectKeys {
		if err := b.client.bucketManager.Delete(b.bucketName, objectKey); err != nil {
			return err
		}
	}

	return nil
}
