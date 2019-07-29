package huaweicloud_obs

import (
	"context"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/huaweicloud_obs/obs"
	"io"
	"os"
)

const tempFileSuffix = ".temp"
const filePermMode = os.FileMode(0664) // Default file permission

type obsBucket struct {
	name   string
	bucket *obs.ObsClient
}

func (b *obsBucket) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	input := &obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: b.name,
			Key:    objectKey,
		},
	}
	output, err := b.bucket.GetObject(input)
	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

func (b *obsBucket) FGetObject(ctx context.Context, objectKey string, localPath string) error {
	tempFilePath := localPath + tempFileSuffix

	body, err := b.GetObject(ctx, objectKey)
	if err != nil {
		return err
	}
	defer body.Close()

	fd, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filePermMode)
	if err != nil {
		return err
	}

	_, err = io.Copy(fd, body)
	fd.Close()
	if err != nil {
		return err
	}

	return os.Rename(tempFilePath, localPath)
}

func (b *obsBucket) StatObject(ctx context.Context, objectKey string) (object sdk.ObjectProperties, err error) {
	input := &obs.GetObjectMetadataInput{
		Bucket: b.name,
		Key:    objectKey,
	}
	output, err := b.bucket.GetObjectMetadata(input)
	if err != nil {
		return
	}

	return sdk.ObjectProperties{
		Key:  objectKey,
		Type: output.ObjectType,
		Size: output.ContentLength,
		ETag: output.ETag,
	}, nil
}

func (b *obsBucket) ListObjects(ctx context.Context, objectPrefix string) (objects []sdk.ObjectProperties, err error) {
	input := &obs.ListObjectsInput{
		Bucket: b.name,
		ListObjsInput: obs.ListObjsInput{
			Prefix: objectPrefix,
		},
	}
	output, err := b.bucket.ListObjects(input)
	if err != nil {
		return
	}

	for _, object := range output.Contents {
		objects = append(objects, sdk.ObjectProperties{
			Key:  object.Key,
			Size: object.Size,
			ETag: object.ETag,
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
	_, err := b.bucket.PutObject(input)
	return err
}

func (b *obsBucket) FPutObject(ctx context.Context, objectKey string, filePath string) error {
	input := &obs.PutFileInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: b.name,
				Key:    objectKey,
			},
		},
		SourceFile: filePath,
	}
	_, err := b.bucket.PutFile(input)
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
	_, err := b.bucket.CopyObject(input)
	return err
}

func (b *obsBucket) RemoveObject(ctx context.Context, objectKey string) error {
	input := &obs.DeleteObjectInput{
		Bucket: b.name,
		Key:    objectKey,
	}
	_, err := b.bucket.DeleteObject(input)
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
	_, err := b.bucket.DeleteObjects(input)
	return err
}

func (b *obsBucket) PresignGetObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodGet,
		Bucket:  b.name,
		Key:     objectKey,
		Expires: int(expiredInSec),
	}
	output, err := b.bucket.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}

func (b *obsBucket) PresignHeadObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodHead,
		Bucket:  b.name,
		Key:     objectKey,
		Expires: int(expiredInSec),
	}
	output, err := b.bucket.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}

func (b *obsBucket) PresignPutObject(ctx context.Context, objectKey string, expiredInSec int64) (signedURL string, err error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodPut,
		Bucket:  b.name,
		Key:     objectKey,
		Expires: int(expiredInSec),
	}
	output, err := b.bucket.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}

func (b *obsBucket) PresignPostObject(ctx context.Context, p *sdk.PostPolicy) (signedURL string, formData map[string]string, err error) {
	panic("implement me")
}
