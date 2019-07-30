package tencent_cos

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"time"
)

func newBucket(region, bucketName string, client *cos.Client) *cosBucket {
	return &cosBucket{
		region:     region,
		bucketName: bucketName,
		client:     client,
	}
}

type cosBucket struct {
	region     string
	bucketName string
	client     *cos.Client
}

func (b *cosBucket) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	resp, err := b.client.Object.Get(ctx, objectKey, nil)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (b *cosBucket) StatObject(ctx context.Context, objectKey string) (object sdk.ObjectMeta, err error) {
	resp, err := b.client.Object.Head(ctx, objectKey, nil)
	if err != nil {
		return
	}

	return sdk.HeaderToObjectMeta(resp.Header)
}

func (b *cosBucket) ListObjects(ctx context.Context, objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	ret, _, err := b.client.Bucket.Get(ctx, &cos.BucketGetOptions{
		Prefix: objectPrefix,
	})
	if err != nil {
		return nil, err
	}

	for _, object := range ret.Contents {
		lastModified, err := time.Parse(time.RFC1123, object.LastModified)
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

func (b *cosBucket) PutObject(ctx context.Context, objectKey string, reader io.Reader) error {
	_, err := b.client.Object.Put(ctx, objectKey, reader, nil)
	return err
}

func (b *cosBucket) CopyObject(ctx context.Context, srcObjectKey, dstObjectKey string) error {
	_, _, err := b.client.Object.Copy(ctx, dstObjectKey, objectURL(b.region, b.bucketName, srcObjectKey), nil)
	return err
}

func (b *cosBucket) RemoveObject(ctx context.Context, objectKey string) error {
	_, err := b.client.Object.Delete(ctx, objectKey)
	return err
}

func (b *cosBucket) RemoveObjects(ctx context.Context, objectKeys []string) error {
	objects := make([]cos.Object, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		objects = append(objects, cos.Object{
			Key: objectKey,
		})
	}

	_, _, err := b.client.Object.DeleteMulti(ctx, &cos.ObjectDeleteMultiOptions{
		Objects: objects,
	})
	return err
}
