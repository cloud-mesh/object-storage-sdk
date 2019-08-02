package aws_s3

import (
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/testcase"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestS3Bucket_Object(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketObjectTest(t, bucket)
}

func TestS3Bucket_Objects(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketObjectsTest(t, bucket)
}

func TestS3Bucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignHeadObjectTest(t, bucket)
}

func TestS3Bucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignGetObjectTest(t, bucket)
}

func TestS3Bucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	testcase.BucketPresignPutObjectTest(t, bucket)
}

func newTestBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client, err := NewClient(testRegion, testEndpoint, testAccessKeyId, testAccessKeySecret, nil)
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
