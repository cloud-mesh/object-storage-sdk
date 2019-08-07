package aliyun_oss

import (
	"github.com/inspii/object_storage_sdk/testcase"
	"testing"
)

func TestOssBucket_MultipartUpload(t *testing.T) {
	bucket, destroy := newBucket(t)
	defer destroy()

	testcase.BucketMultipartUploadTest(t, bucket)
}
