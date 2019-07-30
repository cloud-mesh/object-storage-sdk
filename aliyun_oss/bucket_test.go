package aliyun_oss

import (
	"bytes"
	"context"
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
	"time"
)

func TestOssBucket_Object(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	objectKey := fmt.Sprintf("testobjecta%d", time.Now().Unix())
	buffer := bytes.NewBufferString("test content")
	err := bucket.PutObject(context.Background(), objectKey, buffer)
	assert.Nil(t, err)

	object, err := bucket.StatObject(context.Background(), objectKey)
	assert.Nil(t, err)
	assert.Equal(t, objectKey, object.Key)

	_, err = bucket.GetObject(context.Background(), objectKey)
	assert.Nil(t, err)

	err = bucket.RemoveObject(context.Background(), objectKey)
}

func TestOssBucket_Objects(t *testing.T) {
	bucket, destroy := newTestBucket(t)
	defer destroy()

	buffer := bytes.NewBufferString("test content")
	objectKeyA := fmt.Sprintf("testobjecta%d", time.Now().Unix())
	objectKeyB := fmt.Sprintf("testobjectb%d", time.Now().Unix())
	objectKeyC := fmt.Sprintf("testobjectc%d", time.Now().Unix())
	err := bucket.PutObject(context.Background(), objectKeyA, buffer)
	assert.Nil(t, err)
	err = bucket.PutObject(context.Background(), objectKeyB, buffer)
	assert.Nil(t, err)
	err = bucket.CopyObject(context.Background(), objectKeyA, objectKeyC)
	assert.Nil(t, err)

	objects, err := bucket.ListObjects(context.Background(), "testobject")
	assert.Nil(t, err)
	assert.Len(t, objects, 3)

	err = bucket.RemoveObjects(context.Background(), []string{objectKeyA, objectKeyB, objectKeyC})
	assert.Nil(t, err)

	objects, err = bucket.ListObjects(context.Background(), "testobject")
	assert.Nil(t, err)
	assert.Len(t, objects, 0)
}

func TestOssBucket_PresignGetObject(t *testing.T) {
	basicBucket, destroy := newTestBucket(t)
	defer destroy()

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	content := "test content"
	buffer := bytes.NewBufferString(content)
	err := basicBucket.PutObject(context.Background(), objectKey, buffer)
	assert.Nil(t, err)
	defer basicBucket.RemoveObject(context.Background(), objectKey)

	bucket := basicBucket.(sdk.PresignBucket)
	signedURL, err := bucket.PresignGetObject(context.Background(), objectKey, time.Second)
	assert.Nil(t, err)

	reader, err := sdk.GetObjectWithURL(signedURL, 5*time.Second)
	assert.Nil(t, err)
	bytes, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, content, string(bytes))
}

func TestOssBucket_PresignPutObject(t *testing.T) {
	basicBucket, destroy := newTestBucket(t)
	defer destroy()

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	bucket := basicBucket.(sdk.PresignBucket)
	signedURL, err := bucket.PresignPutObject(context.Background(), objectKey, time.Second)
	assert.Nil(t, err)

	content := "test content"
	buffer := bytes.NewBufferString(content)
	err = sdk.PutObjectWithURL(signedURL, buffer, 5*time.Second)
	assert.Nil(t, err)
	defer basicBucket.RemoveObject(context.Background(), objectKey)

	reader, err := basicBucket.GetObject(context.Background(), objectKey)
	assert.Nil(t, err)
	bytes, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, content, string(bytes))
}

func newTestBucket(t *testing.T) (bucket sdk.BasicBucket, destroy func()) {
	client, err := NewClient(testEndpoint, testAccessKeyId, testAccessKeySecret)
	assert.Nil(t, err)

	bucketName := fmt.Sprintf("test%d", time.Now().Unix())
	err = client.MakeBucket(context.Background(), bucketName)
	assert.Nil(t, err)

	bucket, err = client.Bucket(bucketName)
	return bucket, func() {
		time.Sleep(time.Second)
		err := client.RemoveBucket(context.Background(), bucketName)
		assert.Nil(t, err)
	}
}
