package tests

import (
	"bytes"
	"context"
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func ClientBucketTest(t *testing.T, client sdk.BasicClient) {
	bucketName := fmt.Sprintf("testbucket%d", time.Now().Unix())
	err := client.MakeBucket(context.Background(), bucketName)
	assert.Nil(t, err)
	assertBucketExist(t, client, bucketName, true)

	err = client.RemoveBucket(context.Background(), bucketName)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	assertBucketExist(t, client, bucketName, false)
}

func ClientCopyObjectTest(t *testing.T, client sdk.BasicClient) {
	bucketNameA := fmt.Sprintf("testbucketa%d", time.Now().Unix())
	bucketNameB := fmt.Sprintf("testbucketb%d", time.Now().Unix())
	err := client.MakeBucket(context.Background(), bucketNameA)
	assert.Nil(t, err)
	defer client.RemoveBucket(context.Background(), bucketNameA)
	err = client.MakeBucket(context.Background(), bucketNameB)
	assert.Nil(t, err)
	defer client.RemoveBucket(context.Background(), bucketNameB)

	bucketA, err := client.Bucket(bucketNameA)
	assert.Nil(t, err)
	bucketB, err := client.Bucket(bucketNameB)
	assert.Nil(t, err)

	objectKeyA := fmt.Sprintf("testobjecta%d.txt", time.Now().Unix())
	objectKeyB := fmt.Sprintf("testobjectb%d.txt", time.Now().Unix())

	buffer := bytes.NewBufferString("test content")
	err = bucketA.PutObject(context.Background(), objectKeyA, buffer)
	assert.Nil(t, err)
	defer bucketA.RemoveObject(context.Background(), objectKeyA)
	err = client.CopyObject(context.Background(), bucketNameA, objectKeyA, bucketNameB, objectKeyB)
	assert.Nil(t, err)
	defer bucketB.RemoveObject(context.Background(), objectKeyB)
	_, err = bucketB.GetObject(context.Background(), objectKeyB)
	assert.Nil(t, err)
}

func assertBucketExist(t *testing.T, client sdk.BasicClient, bucketName string, isExist bool) {
	buckets, err := client.ListBucket(context.Background())
	assert.Nil(t, err)
	var bucketExist bool
	for _, bucket := range buckets {
		if bucket.Name == bucketName {
			bucketExist = true
		}
	}
	assert.Equal(t, isExist, bucketExist)
}
