package testcase

import (
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func BucketMultipartUploadTest(t *testing.T, bucket sdk.BasicBucket) {
	multipartBucket, ok := bucket.(sdk.MultipartUploadAbleBucket)
	if !ok {
		t.Skip()
	}

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	uploadId, err := multipartBucket.InitMultipartUpload(objectKey)
	assert.Nil(t, err)
	_, err = multipartBucket.ListMultipartUploads(objectKey)
	assert.Nil(t, err)

	part1 := strings.Repeat("hello,", 1000000)
	part2 := strings.Repeat("world", 1000000)
	buffer1 := strings.NewReader(part1)
	buffer2 := strings.NewReader(part2)
	_, err = multipartBucket.UploadPart(objectKey, uploadId, 1, buffer1)
	assert.Nil(t, err)
	_, err = multipartBucket.UploadPart(objectKey, uploadId, 2, buffer2)
	assert.Nil(t, err)
	_, err = multipartBucket.ListParts(objectKey, uploadId)
	assert.Nil(t, err)

	err = multipartBucket.CompleteUploadPart(objectKey, uploadId, []sdk.CompletePart{
		{
			PartNumber: 1,
			ETag:       md5str(part1),
		}, {
			PartNumber: 2,
			ETag:       md5str(part2),
		},
	})
	assert.Nil(t, err)

	err = bucket.RemoveObject(objectKey)
	assert.Nil(t, err)
}

func BucketMultipartUploadPresignTest(t *testing.T, bucket sdk.BasicBucket) {
	multipartBucket, ok := bucket.(sdk.MultipartUploadAbleBucket)
	if !ok {
		t.Skip()
	}
	presignBucket, ok := bucket.(sdk.MultipartUploadPresignAbleBucket)
	if !ok {
		t.Skip()
	}

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	uploadId, err := multipartBucket.InitMultipartUpload(objectKey)
	assert.Nil(t, err)

	part1 := strings.Repeat("hello,", 1000000)
	part2 := strings.Repeat("world", 1000000)
	buffer1 := strings.NewReader(part1)
	buffer2 := strings.NewReader(part2)
	part1Url, err := presignBucket.PresignUploadPart(objectKey, uploadId, 1, time.Minute)
	part2Url, err := presignBucket.PresignUploadPart(objectKey, uploadId, 2, time.Minute)

	resp1, err := sdk.PutObjectWithURL(part1Url, buffer1, time.Minute)
	assert.Nil(t, err)
	resp2, err := sdk.PutObjectWithURL(part2Url, buffer2, time.Minute)
	assert.Nil(t, err)

	completeParts := []sdk.CompletePart{
		{
			PartNumber: 1,
			ETag:       resp1.Header.Get("Etag"),
		}, {
			PartNumber: 2,
			ETag:       resp2.Header.Get("Etag"),
		},
	}
	err = multipartBucket.CompleteUploadPart(objectKey, uploadId, completeParts)
	assert.Nil(t, err)

	err = bucket.RemoveObject(objectKey)
	assert.Nil(t, err)
}
