package aliyun_oss

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aws/aws-sdk-go/aws"
	sdk "github.com/cloud-mesh/object-storage-sdk"
	"io"
	"strings"
)

func (c *ossBucket) ListMultipartUploads(objectKeyPrefix string) (uploads []sdk.Upload, err error) {
	option := oss.Prefix(objectKeyPrefix)
	result, err := c.bucket.ListMultipartUploads(option)
	if err != nil {
		return
	}

	for _, item := range result.Uploads {
		uploads = append(uploads, sdk.Upload{
			ObjectKey: item.Key,
			UploadId:  item.UploadID,
			Initiated: item.Initiated,
		})
	}

	return
}

func (c *ossBucket) InitMultipartUpload(objectKey string) (uploadId string, err error) {
	result, err := c.bucket.InitiateMultipartUpload(objectKey)
	if err != nil {
		return
	}

	return result.UploadID, nil
}

func (c *ossBucket) UploadPart(objectKey, uploadId string, partNum int, reader io.ReadSeeker) (string, error) {
	partSize, err := aws.SeekerLen(reader)
	if err != nil {
		return "", err
	}

	imur := oss.InitiateMultipartUploadResult{
		Bucket:   c.bucket.BucketName,
		Key:      objectKey,
		UploadID: uploadId,
	}
	result, err := c.bucket.UploadPart(imur, reader, partSize, partNum)
	if err != nil {
		return "", err
	}

	return result.ETag, nil
}

func (c *ossBucket) ListParts(objectKey string, uploadId string) (parts []sdk.Part, err error) {
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   c.bucket.BucketName,
		Key:      objectKey,
		UploadID: uploadId,
	}

	result, err := c.bucket.ListUploadedParts(imur)
	if err != nil {
		return nil, err
	}

	for _, part := range result.UploadedParts {
		parts = append(parts, sdk.Part{
			PartNumber:   part.PartNumber,
			Size:         part.Size,
			ETag:         strings.ToLower(part.ETag),
			LastModified: part.LastModified,
		})
	}

	return
}

func (c *ossBucket) CompleteUploadPart(objectKey string, uploadId string, parts []sdk.CompletePart) error {
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   c.bucket.BucketName,
		Key:      objectKey,
		UploadID: uploadId,
	}

	ossParts := make([]oss.UploadPart, 0, len(parts))
	for _, part := range parts {
		ossParts = append(ossParts, oss.UploadPart{
			PartNumber: part.PartNumber,
			ETag:       strings.ToUpper(part.ETag),
		})
	}

	_, err := c.bucket.CompleteMultipartUpload(imur, ossParts)
	return err
}

func (c *ossBucket) AbortMultipartUpload(objectKey string, uploadId string) error {
	imur := oss.InitiateMultipartUploadResult{
		Bucket:   c.bucket.BucketName,
		Key:      objectKey,
		UploadID: uploadId,
	}

	return c.bucket.AbortMultipartUpload(imur)
}
