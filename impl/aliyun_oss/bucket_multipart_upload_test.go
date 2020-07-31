package aliyun_oss

import (
	"github.com/cloud-mesh/object-storage-sdk/testcase"
	"testing"
)

func TestOssBucket_MultipartUpload(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadTest(t, bucket)
}
