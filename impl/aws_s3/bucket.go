package aws_s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	sdk "github.com/inspii/object-storage-sdk"
	"io"
	"time"
)

func newS3Bucket(bucketName string, client *s3Client) *s3Bucket {
	return &s3Bucket{bucketName, client}
}

type s3Bucket struct {
	bucketName string
	*s3Client
}

func (b *s3Bucket) GetObject(objectKey string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	}
	output, err := b.client.GetObject(input)
	if err != nil {
		return nil, err
	}

	return output.Body, nil
}

func (b *s3Bucket) HeadObject(objectKey string) (object sdk.ObjectMeta, err error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	}
	output, err := b.client.HeadObject(input)
	if err != nil {
		return
	}

	return sdk.ObjectMeta{
		ContentType:   aws.StringValue(output.ContentType),
		ContentLength: int(aws.Int64Value(output.ContentLength)),
		ETag:          aws.StringValue(output.ETag),
		LastModified:  aws.TimeValue(output.LastModified),
	}, nil
}

func (b *s3Bucket) ListObjects(objectPrefix string) (objects []sdk.ObjectProperty, err error) {
	input := &s3.ListObjectsInput{
		Bucket: aws.String(b.bucketName),
		Prefix: aws.String(objectPrefix),
	}
	output, err := b.client.ListObjects(input)

	for _, object := range output.Contents {
		objects = append(objects, sdk.ObjectProperty{
			ObjectKey: aws.StringValue(object.Key),
			ObjectMeta: sdk.ObjectMeta{
				ContentType:   "",
				ContentLength: int(*object.Size),
				ETag:          aws.StringValue(object.ETag),
				LastModified:  aws.TimeValue(object.LastModified),
			},
		})
	}

	return
}

func (b *s3Bucket) PutObject(objectKey string, reader io.Reader) error {
	if readerSeeker, ok := reader.(io.ReadSeeker); ok {
		input := &s3.PutObjectInput{
			Bucket: aws.String(b.bucketName),
			Key:    aws.String(objectKey),
			Body:   readerSeeker,
		}
		_, err := b.client.PutObject(input)
		return err
	}

	// see: https://github.com/aws/aws-sdk-go/issues/915
	uploader := s3manager.NewUploader(b.session, func(u *s3manager.Uploader) {
		u.Concurrency = 1
	})
	input := &s3manager.UploadInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
		Body:   reader,
	}

	_, err := uploader.Upload(input)
	return err
}

func (b *s3Bucket) CopyObject(srcObjectKey, dstObjectKey string) error {
	copySource := fmt.Sprintf("%s/%s", b.bucketName, srcObjectKey)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(b.bucketName),
		Key:        aws.String(dstObjectKey),
		CopySource: aws.String(copySource),
	}
	_, err := b.client.CopyObject(input)
	return err
}

func (b *s3Bucket) RemoveObject(objectKey string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	}
	_, err := b.client.DeleteObject(input)
	return err
}

func (b *s3Bucket) RemoveObjects(objectKeys []string) error {
	objs := make([]*s3.ObjectIdentifier, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		objs = append(objs, &s3.ObjectIdentifier{
			Key: aws.String(objectKey),
		})
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(b.bucketName),
		Delete: &s3.Delete{
			Objects: objs,
		},
	}
	_, err := b.client.DeleteObjects(input)
	return err
}

func (b *s3Bucket) PresignGetObject(objectKey string, expiresIn time.Duration) (string, error) {
	req, _ := b.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	})
	url, _, err := req.PresignRequest(expiresIn)
	if err != nil {
		return "", err
	}

	return url, nil
}

func (b *s3Bucket) PresignHeadObject(objectKey string, expiresIn time.Duration) (string, error) {
	req, _ := b.client.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	})
	url, _, err := req.PresignRequest(expiresIn)
	if err != nil {
		return "", err
	}

	return url, nil
}

func (b *s3Bucket) PresignPutObject(objectKey string, expiresIn time.Duration) (string, error) {
	req, _ := b.client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	})
	url, _, err := req.PresignRequest(expiresIn)
	if err != nil {
		return "", err
	}

	return url, nil
}
