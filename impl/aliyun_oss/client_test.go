package aliyun_oss

import (
	"github.com/inspii/object_storage_sdk/testcase"
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
	client := newClient(t)

	testcase.ClientBucketTest(t, client)
}

func TestOssClient_CopyObject(t *testing.T) {
	client := newClient(t)

	testcase.ClientCopyObjectTest(t, client)
}

func newClient(t *testing.T) *ossClient {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	return client
}
