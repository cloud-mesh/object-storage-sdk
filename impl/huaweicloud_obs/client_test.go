package huaweicloud_obs

import (
	"fmt"
	"github.com/inspii/object_storage_sdk/testcase"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	testEndpoint = os.Getenv("TEST_OBS_ENDPOINT")
	testLocation = os.Getenv("TEST_OBS_LOCATION")
	testAK       = os.Getenv("TEST_OBS_AK")
	testSK       = os.Getenv("TEST_OBS_SK")
)

func TestObsClient_Bucket(t *testing.T) {
	fmt.Println(testEndpoint)
	client, err := NewClient(testLocation, testEndpoint, testAK, testSK)
	assert.Nil(t, err)

	testcase.ClientBucketTest(t, client)
}

func TestObsClient_CopyObject(t *testing.T) {
	client, err := NewClient(testLocation, testEndpoint, testAK, testSK)
	assert.Nil(t, err)

	testcase.ClientCopyObjectTest(t, client)
}
