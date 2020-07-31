package tencent_cos

import (
	sdk "github.com/cloud-mesh/object-storage-sdk"
	"github.com/cloud-mesh/object-storage-sdk/testcase"
	"testing"
)

func TestCosBucket_Object(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketObjectTest(t, bucket)
}

func TestCosBucket_Objects(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketObjectsTest(t, bucket)
}

func TestCosBucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignHeadObjectTest(t, bucket)
}

func TestCosBucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignGetObjectTest(t, bucket)
}

func TestCosBucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignPutObjectTest(t, bucket)
}

func newBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client := newClient(t)
	return testcase.NewTestBucket(t, client)
}
