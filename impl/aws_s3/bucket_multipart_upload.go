package aws_s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	sdk "github.com/inspii/object_storage_sdk"
	"io"
	"strings"
	"time"
)

func (b *s3Bucket) ListMultipartUploads(objectKeyPrefix string) (uploads []sdk.Upload, err error) {
	input := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(b.bucketName),
		Prefix: aws.String(objectKeyPrefix),
	}

	output, err := b.client.ListMultipartUploads(input)
	if err != nil {
		return
	}

	for _, upload := range output.Uploads {
		uploads = append(uploads, sdk.Upload{
			ObjectKey: *upload.Key,
			UploadId:  *upload.UploadId,
			Initiated: *upload.Initiated,
		})
	}

	return
}

func (b *s3Bucket) InitMultipartUpload(objectKey string) (uploadId string, err error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectKey),
	}
	output, err := b.client.CreateMultipartUpload(input)
	if err != nil {
		return
	}

	return *output.UploadId, nil
}

func (b *s3Bucket) UploadPart(objectKey, uploadId string, partNum int, reader io.ReadSeeker) error {
	input := &s3.UploadPartInput{
		Bucket:     aws.String(b.bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int64(int64(partNum)),
		Body:       reader,
	}
	_, err := b.client.UploadPart(input)
	return err
}

func (b *s3Bucket) ListParts(objectKey string, uploadId string) (parts []sdk.Part, err error) {
	input := &s3.ListPartsInput{
		Bucket:   aws.String(b.bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadId),
	}
	output, err := b.client.ListParts(input)
	if err != nil {
		return
	}

	for _, part := range output.Parts {
		parts = append(parts, sdk.Part{
			PartNumber:   int(*part.PartNumber),
			Size:         int(*part.Size),
			ETag:         strings.ToLower(*part.ETag),
			LastModified: *part.LastModified,
		})
	}
	return
}

func (b *s3Bucket) CompleteUploadPart(objectKey string, uploadId string, parts []sdk.CompletePart) error {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(b.bucketName),
		Key:             aws.String(objectKey),
		UploadId:        aws.String(uploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{},
	}
	for _, part := range parts {
		input.MultipartUpload.Parts = append(input.MultipartUpload.Parts, &s3.CompletedPart{
			PartNumber: aws.Int64(int64(part.PartNumber)),
			ETag:       aws.String(strings.ToLower(part.ETag)),
		})
	}

	_, err := b.client.CompleteMultipartUpload(input)
	return err
}

func (b *s3Bucket) AbortMultipartUpload(objectKey string, uploadId string) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadId),
	}
	_, err := b.client.AbortMultipartUpload(input)
	return err
}

func (b *s3Bucket) PresignUploadPart(objectKey string, uploadId string, partNum int, expiresIn time.Duration) (string, error) {
	req, _ := b.client.UploadPartRequest(&s3.UploadPartInput{
		Bucket:     aws.String(b.bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int64(int64(partNum)),
	})
	url, _, err := req.PresignRequest(expiresIn)
	if err != nil {
		return "", err
	}

	return url, nil
}
