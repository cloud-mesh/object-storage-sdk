package huaweicloud_obs

import (
	"github.com/inspii/object-storage-sdk/impl/huaweicloud_obs/obs"
	"github.com/inspii/object-storage-sdk/testcase"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	testOBS      = os.Getenv("TEST_OBS")
	testEndpoint = os.Getenv("TEST_OBS_ENDPOINT")
	testLocation = os.Getenv("TEST_OBS_LOCATION")
	testAK       = os.Getenv("TEST_OBS_AK")
	testSK       = os.Getenv("TEST_OBS_SK")
)

func TestObsClient_Bucket(t *testing.T) {
	client := newClient(t)

	testcase.ClientBucketTest(t, client)
}

func TestObsClient_CopyObject(t *testing.T) {
	client := newClient(t)

	testcase.ClientCopyObjectTest(t, client)
}

func newClient(t *testing.T) *obsClient {
	if testOBS != "true" {
		t.Skip()
	}
	obsClient, err := obs.New(testAK, testSK, testEndpoint)
	assert.Nil(t, err)

	return NewClient(testLocation, obsClient)
}
