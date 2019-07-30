package huaweicloud_obs

import (
	"context"
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/tests"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestObsBucket_Object(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	tests.BucketObjectTest(t, bucket)
}

func TestObsBucket_Objects(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	tests.BucketObjectsTest(t, bucket)
}

func TestObsBucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	tests.BucketPresignHeadObjectTest(t, bucket)
}

func TestObsBucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	tests.BucketPresignGetObjectTest(t, bucket)
}

func TestObsBucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	tests.BucketPresignPutObjectTest(t, bucket)
}

func newTestBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client, err := NewClient(testEndpoint, testAk, testSk)
	assert.Nil(t, err)

	bucketName := fmt.Sprintf("testbucket%d", time.Now().Unix())
	err = client.MakeBucket(context.Background(), bucketName)
	assert.Nil(t, err)

	bucket, err = client.Bucket(bucketName)
	return bucket, func() {
		time.Sleep(time.Second)
		err := client.RemoveBucket(context.Background(), bucketName)
		assert.Nil(t, err)
	}
}
