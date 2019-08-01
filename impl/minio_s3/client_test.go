package minio_s3

import (
	"github.com/inspii/object_storage_sdk/testcase"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	testRegion          = os.Getenv("TEST_MINIO_REGION")
	testEndpoint        = os.Getenv("TEST_MINIO_ENDPOINT")
	testAccessKeyId     = os.Getenv("TEST_MINIO_ACCESS_KEY_ID")
	testAccessKeySecret = os.Getenv("TEST_MINIO_ACCESS_KEY_SECRET")
)

func TestMinioClient_Bucket(t *testing.T) {
	client, err := NewClient(testRegion, testEndpoint, testAccessKeyId, testAccessKeySecret, false)
	assert.Nil(t, err)

	testcase.ClientBucketTest(t, client)
}

func TestMinioClient_CopyObject(t *testing.T) {
	client, err := NewClient(testRegion, testEndpoint, testAccessKeyId, testAccessKeySecret, false)
	assert.Nil(t, err)

	testcase.ClientCopyObjectTest(t, client)
}
