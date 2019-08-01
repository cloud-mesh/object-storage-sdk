package tencent_cos

import (
	"github.com/inspii/object_storage_sdk/testcase"
	"os"
	"testing"
)

var (
	testRegion    = os.Getenv("TEST_COS_REGION")
	testAppId     = os.Getenv("TEST_COS_APP_ID")
	testSecretId  = os.Getenv("TEST_COS_SECRET_ID")
	testSecretKey = os.Getenv("TEST_COS_SECRET_KEY")
)

func TestCosClient_Bucket(t *testing.T) {
	client := NewClient(testRegion, testAppId, testSecretId, testSecretKey)
	testcase.ClientBucketTest(t, client)
}

func TestCosClient_CopyObject(t *testing.T) {
	client := NewClient(testRegion, testAppId, testSecretId, testSecretKey)
	testcase.ClientCopyObjectTest(t, client)
}
