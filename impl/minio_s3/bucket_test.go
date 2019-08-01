package minio_s3

import (
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/testcase"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMinioBucket_Object(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketObjectTest(t, bucket)
}

func TestMinioBucket_Objects(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketObjectsTest(t, bucket)
}

func TestMinioBucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignHeadObjectTest(t, bucket)
}

func TestMinioBucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignGetObjectTest(t, bucket)
}

func TestMinioBucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignPutObjectTest(t, bucket)
}

func newTestBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client, err := NewClient(testRegion, testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	bucketName := fmt.Sprintf("testbucket%d", time.Now().Unix())
	err = client.MakeBucket(bucketName)
	assert.Nil(t, err)

	bucket, err = client.Bucket(bucketName)
	return bucket, func() {
		time.Sleep(time.Second)
		err := client.RemoveBucket(bucketName)
		assert.Nil(t, err)
	}
}
