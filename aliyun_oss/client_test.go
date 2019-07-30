package aliyun_oss

import (
	"github.com/inspii/object_storage_sdk/tests"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	testEndpoint        = os.Getenv("TEST_OSS_ENDPOINT")
	testAccessKeyId     = os.Getenv("TEST_OSS_ACCESS_KEY_ID")
	testAccessKeySecret = os.Getenv("TEST_OSS_ACCESS_KEY_SECRET")
)

func TestOssClient_Bucket(t *testing.T) {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	tests.ClientBucketTest(t, client)
}

func TestOssClient_CopyObject(t *testing.T) {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	tests.ClientCopyObjectTest(t, client)
}
