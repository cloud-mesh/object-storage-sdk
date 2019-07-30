package huaweicloud_obs

import (
	"github.com/inspii/object_storage_sdk/tests"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	testEndpoint = os.Getenv("TEST_OBS_ENDPOINT")
	testAk       = os.Getenv("TEST_OBS_AK")
	testSk       = os.Getenv("TEST_OBS_SK")
)

func TestObsClient_Bucket(t *testing.T) {
	client, err := NewClient(testEndpoint, testAk, testSk)
	assert.Nil(t, err)

	tests.ClientBucketTest(t, client)
}

func TestObsClient_CopyObject(t *testing.T) {
	client, err := NewClient(testEndpoint, testAk, testSk)
	assert.Nil(t, err)

	tests.ClientCopyObjectTest(t, client)
}
