package tencent_cos

import (
	sdk "github.com/inspii/object-storage-sdk"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"strings"
	"time"
)

func (b *cosBucket) ListMultipartUploads(objectKeyPrefix string) (uploads []sdk.Upload, err error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	opt := &cos.ListMultipartUploadsOptions{
		Prefix: objectKeyPrefix,
	}
	result, _, err := b.client.Bucket.ListMultipartUploads(ctx, opt)
	if err != nil {
		return
	}

	for _, upload := range result.Uploads {
		initiated, err := time.Parse(gmtIso8601, upload.Initiated)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, sdk.Upload{
			ObjectKey: upload.Key,
			UploadId:  upload.UploadID,
			Initiated: initiated,
		})
	}
	return
}

func (b *cosBucket) InitMultipartUpload(objectKey string) (uploadId string, err error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	result, _, err := b.client.Object.InitiateMultipartUpload(ctx, objectKey, nil)
	if err != nil {
		return
	}

	return result.UploadID, nil
}

func (b *cosBucket) UploadPart(objectKey, uploadId string, partNum int, reader io.ReadSeeker) (string, error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	result, err := b.client.Object.UploadPart(ctx, objectKey, uploadId, partNum, reader, nil)
	if err != nil {
		return "", err
	}

	return result.Header.Get("Etag"), nil
}

func (b *cosBucket) ListParts(objectKey string, uploadId string) (parts []sdk.Part, err error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	result, _, err := b.client.Object.ListParts(ctx, objectKey, uploadId, nil)
	if err != nil {
		return
	}

	for _, part := range result.Parts {
		lastModified, err := time.Parse(gmtIso8601, part.LastModified)
		if err != nil {
			return nil, err
		}
		parts = append(parts, sdk.Part{
			PartNumber:   part.PartNumber,
			Size:         part.Size,
			ETag:         strings.ToLower(part.ETag),
			LastModified: lastModified,
		})
	}
	return
}

func (b *cosBucket) CompleteUploadPart(objectKey string, uploadId string, parts []sdk.CompletePart) error {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	opt := &cos.CompleteMultipartUploadOptions{}
	for _, part := range parts {
		opt.Parts = append(opt.Parts, cos.Object{
			PartNumber: part.PartNumber,
			ETag:       strings.ToLower(part.ETag),
		})
	}
	_, _, err := b.client.Object.CompleteMultipartUpload(ctx, objectKey, uploadId, opt)
	return err
}

func (b *cosBucket) AbortMultipartUpload(objectKey string, uploadId string) error {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	_, err := b.client.Object.AbortMultipartUpload(ctx, objectKey, uploadId)
	return err
}

func (b *cosBucket) PresignUploadPart(objectKey string, uploadId string, partNum int, expiresIn time.Duration) (string, error) {
	ctx, cancel := b.config.NewContext()
	defer cancel()

	opt := struct {
		UploadId   string `url:"uploadId"`
		PartNumber int    `url:"partNumber"`
	}{
		UploadId:   uploadId,
		PartNumber: partNum,
	}
	url, err := b.client.Object.GetPresignedURL(ctx, http.MethodPut, objectKey, b.secretId, b.secretKey, expiresIn, opt)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}
