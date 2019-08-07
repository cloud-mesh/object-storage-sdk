package huaweicloud_obs

import (
	"github.com/aws/aws-sdk-go/aws"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/impl/huaweicloud_obs/obs"
	"io"
	"strconv"
	"strings"
	"time"
)

func (b *obsBucket) ListMultipartUploads(objectKeyPrefix string) (uploads []sdk.Upload, err error) {
	input := &obs.ListMultipartUploadsInput{
		Bucket: b.bucketName,
		Prefix: objectKeyPrefix,
	}

	output, err := b.client.ListMultipartUploads(input)
	if err != nil {
		return nil, err
	}

	for _, upload := range output.Uploads {
		uploads = append(uploads, sdk.Upload{
			ObjectKey: upload.Key,
			UploadId:  upload.UploadId,
			Initiated: upload.Initiated,
		})
	}

	return
}

func (b *obsBucket) InitMultipartUpload(objectKey string) (string, error) {
	input := &obs.InitiateMultipartUploadInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: b.bucketName,
			Key:    objectKey,
		},
	}
	output, err := b.client.InitiateMultipartUpload(input)
	if err != nil {
		return "", err
	}

	return output.UploadId, nil
}

func (b *obsBucket) UploadPart(objectKey string, uploadId string, partNum int, reader io.ReadSeeker) error {
	partSize, _ := aws.SeekerLen(reader)
	input := &obs.UploadPartInput{
		Bucket:     b.bucketName,
		Key:        objectKey,
		UploadId:   uploadId,
		PartNumber: partNum,
		PartSize:   partSize,
		Body:       reader,
	}

	_, err := b.client.UploadPart(input)
	return err
}

func (b *obsBucket) ListParts(objectKey string, uploadId string) (parts []sdk.Part, err error) {
	input := &obs.ListPartsInput{
		Bucket:   b.bucketName,
		Key:      objectKey,
		UploadId: uploadId,
	}
	output, err := b.client.ListParts(input)
	if err != nil {
		return nil, err
	}

	for _, part := range output.Parts {
		parts = append(parts, sdk.Part{
			PartNumber:   part.PartNumber,
			ETag:         strings.ToLower(part.ETag),
			LastModified: part.LastModified,
			Size:         int(part.Size),
		})
	}

	return
}

func (b *obsBucket) CompleteUploadPart(objectKey string, uploadId string, parts []sdk.CompletePart) error {
	input := &obs.CompleteMultipartUploadInput{
		Bucket:   b.bucketName,
		Key:      objectKey,
		UploadId: uploadId,
	}
	for _, part := range parts {
		input.Parts = append(input.Parts, obs.Part{
			PartNumber: part.PartNumber,
			ETag:       strings.ToLower(part.ETag),
		})
	}

	_, err := b.client.CompleteMultipartUpload(input)
	return err
}

func (b *obsBucket) AbortMultipartUpload(objectKey string, uploadId string) error {
	input := &obs.AbortMultipartUploadInput{
		Bucket:   b.bucketName,
		Key:      objectKey,
		UploadId: uploadId,
	}
	_, err := b.client.AbortMultipartUpload(input)
	return err
}

func (b *obsBucket) PresignUploadPart(objectKey string, uploadId string, partNum int, expiresIn time.Duration) (string, error) {
	input := &obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodPut,
		Bucket:  b.bucketName,
		Key:     objectKey,
		Expires: int(expiresIn / time.Second),
		QueryParams: map[string]string{
			"partNumber": strconv.Itoa(partNum),
			"uploadId":   uploadId,
		},
	}
	output, err := b.client.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return output.SignedUrl, nil
}
