package aws_s3

import (
	sdk "github.com/cloud-mesh/object-storage-sdk"
	"github.com/cloud-mesh/object-storage-sdk/testcase"
	"testing"
)

func TestS3Bucket_Object(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketObjectTest(t, bucket)
}

func TestS3Bucket_Objects(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketObjectsTest(t, bucket)
}

func TestS3Bucket_PresignHeadObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignHeadObjectTest(t, bucket)
}

func TestS3Bucket_PresignGetObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignGetObjectTest(t, bucket)
}

func TestS3Bucket_PresignPutObject(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketPresignPutObjectTest(t, bucket)
}

func newBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client := newClient(t)
	return testcase.NewTestBucket(t, client)
}
