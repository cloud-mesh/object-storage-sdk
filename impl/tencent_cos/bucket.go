package tencent_cos

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"time"
)

func newBucket(bucketName string, client *cosClient) (*cosBucket, error) {
	c, err := client.bucketClient(bucketName)
	if err != nil {
		return nil, err
	}
	return &cosBucket{
		cosClient:  client,
		bucketName: bucketName,
		client:     c,
	}, nil
}

type cosBucket struct {
	*cosClient
	bucketName string
	client     *cos.Client
}

func (b *cosBucket) GetObject(objectKey string) (io.ReadCloser, error) {
	resp, err := b.client.Object.Get(context.TODO(), objectKey, nil)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (b *cosBucket) StatObject(objectKey string) (object sdk.ObjectMeta, err error) {
	resp, err := b.client.Object.Head(context.TODO(), objectKey, nil)
	if err != nil {
		return
	}

	return sdk.HeaderToObjectMeta(resp.Header)
}

func (b *cosBucket) ListObjects(objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	ret, _, err := b.client.Bucket.Get(context.TODO(), &cos.BucketGetOptions{
		Prefix: objectPrefix,
	})
	if err != nil {
		return nil, err
	}

	for _, object := range ret.Contents {
		lastModified, err := time.Parse(gmtIso8601, object.LastModified)
		if err != nil {
			return nil, err
		}
		objects = append(objects, sdk.ObjectProperty{
			ObjectKey: object.Key,
			ObjectMeta: sdk.ObjectMeta{
				ContentType:   "",
				ContentLength: object.Size,
				ETag:          object.ETag,
				LastModified:  lastModified,
			},
		})
	}

	return
}

func (b *cosBucket) PutObject(objectKey string, reader io.Reader, objectSize int) error {
	_, err := b.client.Object.Put(context.TODO(), objectKey, reader, nil)
	return err
}

func (b *cosBucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	_, _, err := b.client.Object.Copy(context.TODO(), dstObjectKey, objectURI(b.region, b.appId, b.bucketName, srcObjectKey), nil)
	return err
}

func (b *cosBucket) RemoveObject(objectKey string) error {
	_, err := b.client.Object.Delete(context.TODO(), objectKey)
	return err
}

func (b *cosBucket) RemoveObjects(objectKeys []string) error {
	objects := make([]cos.Object, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		objects = append(objects, cos.Object{
			Key: objectKey,
		})
	}

	_, _, err := b.client.Object.DeleteMulti(context.TODO(), &cos.ObjectDeleteMultiOptions{
		Objects: objects,
	})
	return err
}

func (b *cosBucket) PresignGetObject(objectKey string, expiresIn time.Duration) (string, error) {
	url, err := b.client.Object.GetPresignedURL(context.TODO(), http.MethodGet, objectKey, b.secretId, b.secretKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (b *cosBucket) PresignHeadObject(objectKey string, expiresIn time.Duration) (string, error) {
	url, err := b.client.Object.GetPresignedURL(context.TODO(), http.MethodHead, objectKey, b.secretId, b.secretKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (b *cosBucket) PresignPutObject(objectKey string, expiresIn time.Duration) (string, error) {
	url, err := b.client.Object.GetPresignedURL(context.TODO(), http.MethodPut, objectKey, b.secretId, b.secretKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}
