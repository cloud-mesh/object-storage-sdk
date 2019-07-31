package aliyun_oss

import (
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/testcase"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOssBucket_Object(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketObjectTest(t, bucket)
}

func TestOssBucket_Objects(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketObjectsTest(t, bucket)
}

func TestOssBucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignHeadObjectTest(t, bucket)
}

func TestOssBucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignGetObjectTest(t, bucket)
}

func TestOssBucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignPutObjectTest(t, bucket)
}

func newTestBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
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
