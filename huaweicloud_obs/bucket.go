package huaweicloud_obs

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/huaweicloud_obs/obs"
	"io"
	"time"
)

func newObsBucket(client *obs.ObsClient, bucketName string) (*obsBucket, error) {
	return &obsBucket{name: bucketName, client: client}, nil
}

type obsBucket struct {
	name   string
	client *obs.ObsClient
}

func (b *obsBucket) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	input := &obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: b.name,
			Key:    objectKey,
		},
	}
	output, err := b.client.GetObject(input)
	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

func (b *obsBucket) StatObject(ctx context.Context, objectKey string) (object sdk.ObjectMeta, err error) {
	input := &obs.GetObjectMetadataInput{
		Bucket: b.name,
		Key:    objectKey,
	}
	output, err := b.client.GetObjectMetadata(input)
	if err != nil {
		return
	}

	return sdk.ObjectMeta{
		ContentType:   output.ObjectType,
		ContentLength: int(output.ContentLength),
		ETag:          output.ETag,
		LastModified:  output.LastModified,
	}, nil
}

func (b *obsBucket) ListObjects(ctx context.Context, objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	input := &obs.ListObjectsInput{
		Bucket: b.name,
		ListObjsInput: obs.ListObjsInput{
			Prefix: objectPrefix,
		},
	}
	output, err := b.client.ListObjects(input)
	if err != nil {
		return
	}

	for _, object := range output.Contents {
		property, err := b.StatObject(ctx, object.Key)
		if err != nil {
			return
		}

		objects = append(objects, sdk.ObjectProperty{
			ObjectKey:  object.Key,
			ObjectMeta: property,
		})
	}

	return
}

func (b *obsBucket) PutObject(ctx context.Context, objectKey string, reader io.Reader) error {
	input := &obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: b.name,
				Key:    objectKey,
			},
		},
		Body: reader,
	}
	_, err := b.client.PutObject(input)
	return err
}

func (b *obsBucket) CopyObject(ctx context.Context, srcObjectKey, dstObjectKey string) error {
	input := &obs.CopyObjectInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: b.name,
			Key:    dstObjectKey,
		},
		CopySourceBucket: b.name,
		CopySourceKey:    srcObjectKey,
	}
	_, err := b.client.CopyObject(input)
	return err
}

func (b *obsBucket) RemoveObject(ctx context.Context, objectKey string) error {
	input := &obs.DeleteObjectInput{
		Bucket: b.name,
		Key:    objectKey,
	}
	_, err := b.client.DeleteObject(input)
	return err
}

func (b *obsBucket) RemoveObjects(ctx context.Context, objectKeys []string) error {
	objects := make([]obs.ObjectToDelete, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		objects = append(objects, obs.ObjectToDelete{
			Key: objectKey,
		})
	}
	input := &obs.DeleteObjectsInput{
		Bucket:  b.name,
		Objects: objects,
	}
	_, err := b.client.DeleteObjects(input)
	return err
}

func (b *obsBucket) PresignGetObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodGet,
		Bucket:  b.name,
		Key:     objectKey,
		Expires: int(expiresIn / time.Second),
	}
	output, err := b.client.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}

func (b *obsBucket) PresignHeadObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodHead,
		Bucket:  b.name,
		Key:     objectKey,
		Expires: int(expiresIn / time.Second),
	}
	output, err := b.client.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}

func (b *obsBucket) PresignPutObject(ctx context.Context, objectKey string, expiresIn time.Duration) (signedURL string, err error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodPut,
		Bucket:  b.name,
		Key:     objectKey,
		Expires: int(expiresIn / time.Second),
	}
	output, err := b.client.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}
