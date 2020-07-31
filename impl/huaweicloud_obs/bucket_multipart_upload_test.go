package huaweicloud_obs

import (
	"github.com/cloud-mesh/object-storage-sdk/testcase"
	"testing"
)

func TestObsBucket_MultipartUpload(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadTest(t, bucket)
}

func TestObsBucket_MultipartUploadPresign(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadPresignTest(t, bucket)
}