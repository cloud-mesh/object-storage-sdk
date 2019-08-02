package testcase

import (
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func ClientBucketTest(t *testing.T, client sdk.BasicClient) {
	bucketName := fmt.Sprintf("testbucket%d", time.Now().Unix())
	err := client.MakeBucket(bucketName)

	err = client.HeadBucket(bucketName)
	assert.Nil(t, err)

	_, err = client.ListBucket()
	assert.Nil(t, err)

	err = client.RemoveBucket(bucketName)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	err = client.HeadBucket(bucketName)
	assert.NotNil(t, err)
}

func ClientCopyObjectTest(t *testing.T, client sdk.BasicClient) {
	bucketNameA := fmt.Sprintf("testbucketa%d", time.Now().Unix())
	bucketNameB := fmt.Sprintf("testbucketb%d", time.Now().Unix())
	err := client.MakeBucket(bucketNameA)
	assert.Nil(t, err)
	defer client.RemoveBucket(bucketNameA)
	err = client.MakeBucket(bucketNameB)
	assert.Nil(t, err)
	defer client.RemoveBucket(bucketNameB)

	bucketA, err := client.Bucket(bucketNameA)
	assert.Nil(t, err)
	bucketB, err := client.Bucket(bucketNameB)
	assert.Nil(t, err)

	objectKeyA := fmt.Sprintf("testobjecta%d.txt", time.Now().Unix())
	objectKeyB := fmt.Sprintf("testobjectb%d.txt", time.Now().Unix())

	buffer := strings.NewReader("test content")
	err = bucketA.PutObject(objectKeyA, buffer)
	assert.Nil(t, err)
	defer bucketA.RemoveObject(objectKeyA)
	err = client.CopyObject(bucketNameA, objectKeyA, bucketNameB, objectKeyB)
	assert.Nil(t, err)
	defer bucketB.RemoveObject(objectKeyB)
	_, err = bucketB.GetObject(objectKeyB)
	assert.Nil(t, err)
}
