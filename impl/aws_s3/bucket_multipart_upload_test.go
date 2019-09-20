package aws_s3

import (
	"github.com/inspii/object-storage-sdk/testcase"
	"testing"
)

func TestS3Bucket_MultipartUpload(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadTest(t, bucket)
}

func TestS3Bucket_MultipartUploadPresign(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadPresignTest(t, bucket)
}
