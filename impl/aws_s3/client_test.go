package aws_s3

import (
	"github.com/inspii/object_storage_sdk/testcase"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	testRegion          = os.Getenv("TEST_S3_REGION")
	testEndpoint        = os.Getenv("TEST_S3_ENDPOINT")
	testAccessKeyId     = os.Getenv("TEST_S3_ACCESS_KEY_ID")
	testAccessKeySecret = os.Getenv("TEST_S3_ACCESS_KEY_SECRET")
)

func TestS3Client_Bucket(t *testing.T) {
	client, err := NewClient(testRegion, testEndpoint, testAccessKeyId, testAccessKeySecret, nil)
	assert.Nil(t, err)

	testcase.ClientBucketTest(t, client)
}

func TestS3Client_CopyObject(t *testing.T) {
	client, err := NewClient(testRegion, testEndpoint, testAccessKeyId, testAccessKeySecret, nil)
	assert.Nil(t, err)

	testcase.ClientCopyObjectTest(t, client)
}
