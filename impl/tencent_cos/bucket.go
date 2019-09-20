package tencent_cos

import (
	sdk "github.com/inspii/object-storage-sdk"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"time"
)

func newCosBucket(bucketName string, client *cosClient) (*cosBucket, error) {
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
	ctx, cancel := b.config.NewContext()
	defer cancel()

	resp, err := b.client.Object.Get(ctx, objectKey, nil)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (b *cosBucket) HeadObject(objectKey string) (object sdk.ObjectMeta, err error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	resp, err := b.client.Object.Head(ctx, objectKey, nil)
	if err != nil {
		return
	}

	return sdk.HeaderToObjectMeta(resp.Header)
}

func (b *cosBucket) ListObjects(objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	ret, _, err := b.client.Bucket.Get(ctx, &cos.BucketGetOptions{
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

func (b *cosBucket) PutObject(objectKey string, reader io.ReadSeeker) error {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	_, err := b.client.Object.Put(ctx, objectKey, reader, nil)
	return err
}

func (b *cosBucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	_, _, err := b.client.Object.Copy(ctx, dstObjectKey, objectURI(b.region, b.appId, b.bucketName, srcObjectKey), nil)
	return err
}

func (b *cosBucket) RemoveObject(objectKey string) error {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	_, err := b.client.Object.Delete(ctx, objectKey)
	return err
}

func (b *cosBucket) RemoveObjects(objectKeys []string) error {
	ctx, cancel := b.config.NewContext()
	defer cancel()

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

func (b *cosBucket) PresignGetObject(objectKey string, expiresIn time.Duration) (string, error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	url, err := b.client.Object.GetPresignedURL(ctx, http.MethodGet, objectKey, b.secretId, b.secretKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (b *cosBucket) PresignHeadObject(objectKey string, expiresIn time.Duration) (string, error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	url, err := b.client.Object.GetPresignedURL(ctx, http.MethodHead, objectKey, b.secretId, b.secretKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (b *cosBucket) PresignPutObject(objectKey string, expiresIn time.Duration) (string, error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	url, err := b.client.Object.GetPresignedURL(ctx, http.MethodPut, objectKey, b.secretId, b.secretKey, expiresIn, nil)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}
