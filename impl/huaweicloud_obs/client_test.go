package huaweicloud_obs

import (
	"github.com/inspii/object_storage_sdk/impl/huaweicloud_obs/obs"
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
	obsClient, err := obs.New(testAK, testSK, testEndpoint)
	assert.Nil(t, err)
	client := NewClient(testLocation, obsClient)

	testcase.ClientBucketTest(t, client)
}

func TestObsClient_CopyObject(t *testing.T) {
	obsClient, err := obs.New(testAK, testSK, testEndpoint)
	assert.Nil(t, err)
	client := NewClient(testLocation, obsClient)

	testcase.ClientCopyObjectTest(t, client)
}
