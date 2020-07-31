package tencent_cos

import (
	"github.com/cloud-mesh/object-storage-sdk/testcase"
	"testing"
)

func TestCosBucket_MultipartUpload(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadTest(t, bucket)
}

func TestCosBucket_MultipartUploadPresign(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadPresignTest(t, bucket)
}