package aliyun_oss

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var (
	testEndpoint        = os.Getenv("TEST_OSS_ENDPOINT")
	testAccessKeyId     = os.Getenv("TEST_OSS_ACCESS_KEY_ID")
	testAccessKeySecret = os.Getenv("TEST_OSS_ACCESS_KEY_SECRET")
)

func TestOssClient_BucketCURD(t *testing.T) {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	bucketName := fmt.Sprintf("test%d", time.Now().Unix())
	err = client.MakeBucket(context.Background(), bucketName)
	assert.Nil(t, err)
	assertBucketExist(t, client, bucketName, true)

	err = client.RemoveBucket(context.Background(), bucketName)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	assertBucketExist(t, client, bucketName, false)
}

func TestOssClient_CopyObject(t *testing.T) {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	bucketNameA := fmt.Sprintf("testa%d", time.Now().Unix())
	bucketNameB := fmt.Sprintf("testb%d", time.Now().Unix())
	err = client.MakeBucket(context.Background(), bucketNameA)
	assert.Nil(t, err)
	defer client.RemoveBucket(context.Background(), bucketNameA)
	err = client.MakeBucket(context.Background(), bucketNameB)
	assert.Nil(t, err)
	defer client.RemoveBucket(context.Background(), bucketNameB)

	bucketA, err := client.Bucket(bucketNameA)
	assert.Nil(t, err)
	bucketB, err := client.Bucket(bucketNameB)
	assert.Nil(t, err)

	objectKeyA := fmt.Sprintf("testobjecta%d", time.Now().Unix())
	objectKeyB := fmt.Sprintf("testobjectb%d", time.Now().Unix())

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

func assertBucketExist(t *testing.T, client *ossClient, bucketName string, isExist bool) {
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
