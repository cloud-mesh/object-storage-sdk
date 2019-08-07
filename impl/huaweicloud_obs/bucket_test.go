package huaweicloud_obs

import (
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/testcase"
	"testing"
)

func TestObsBucket_Object(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketObjectTest(t, bucket)
}

func TestObsBucket_Objects(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketObjectsTest(t, bucket)
}

func TestObsBucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignHeadObjectTest(t, bucket)
}

func TestObsBucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignGetObjectTest(t, bucket)
}

func TestObsBucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignPutObjectTest(t, bucket)
}

func newBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client := newClient(t)

	return testcase.NewTestBucket(t, client)
}
