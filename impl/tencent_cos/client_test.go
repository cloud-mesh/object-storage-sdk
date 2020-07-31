package tencent_cos

import (
	"github.com/inspii/object-storage-sdk/testcase"
	"os"
	"testing"
)

var (
	testCOS       = os.Getenv("TEST_COS")
	testRegion    = os.Getenv("TEST_COS_REGION")
	testAppId     = os.Getenv("TEST_COS_APP_ID")
	testSecretId  = os.Getenv("TEST_COS_SECRET_ID")
	testSecretKey = os.Getenv("TEST_COS_SECRET_KEY")
)

func TestCosClient_Bucket(t *testing.T) {
	client := newClient(t)
	testcase.ClientBucketTest(t, client)
}

func TestCosClient_CopyObject(t *testing.T) {
	client := newClient(t)
	testcase.ClientCopyObjectTest(t, client)
}

func newClient(t *testing.T) *cosClient {
	if testCOS != "true" {
		t.Skip()
	}
	client := NewClient(testRegion, testAppId, testSecretId, testSecretKey)
	return client
}
