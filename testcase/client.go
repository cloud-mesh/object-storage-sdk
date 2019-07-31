package testcase

import (
	"bytes"
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func ClientBucketTest(t *testing.T, client sdk.BasicClient) {
	bucketName := fmt.Sprintf("testbucket%d", time.Now().Unix())
	err := client.MakeBucket(bucketName)
	assert.Nil(t, err)
	assertBucketExist(t, client, bucketName, true)

	err = client.RemoveBucket(bucketName)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	assertBucketExist(t, client, bucketName, false)
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

	buffer := bytes.NewBufferString("test content")
	err = bucketA.PutObject(objectKeyA, buffer)
	assert.Nil(t, err)
	defer bucketA.RemoveObject(objectKeyA)
	err = client.CopyObject(bucketNameA, objectKeyA, bucketNameB, objectKeyB)
	assert.Nil(t, err)
	defer bucketB.RemoveObject(objectKeyB)
	_, err = bucketB.GetObject(objectKeyB)
	assert.Nil(t, err)
}

func assertBucketExist(t *testing.T, client sdk.BasicClient, bucketName string, isExist bool) {
	buckets, err := client.ListBucket()
	assert.Nil(t, err)
	var bucketExist bool
	for _, bucket := range buckets {
		if bucket.Name == bucketName {
			bucketExist = true
		}
	}
	assert.Equal(t, isExist, bucketExist)
}
