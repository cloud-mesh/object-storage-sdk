package tests

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

func BucketObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	objectKey := fmt.Sprintf("testobjecta%d.txt", time.Now().Unix())
	buffer := bytes.NewBufferString("test content")
	err := bucket.PutObject(context.Background(), objectKey, buffer)
	assert.Nil(t, err)

	_, err = bucket.StatObject(context.Background(), objectKey)
	assert.Nil(t, err)

	_, err = bucket.GetObject(context.Background(), objectKey)
	assert.Nil(t, err)

	err = bucket.RemoveObject(context.Background(), objectKey)
}

func BucketObjectsTest(t *testing.T, bucket sdk.BasicBucket) {
	buffer := bytes.NewBufferString("test content")
	objectKeyA := fmt.Sprintf("testobjecta%d.txt", time.Now().Unix())
	objectKeyB := fmt.Sprintf("testobjectb%d.txt", time.Now().Unix())
	objectKeyC := fmt.Sprintf("testobjectc%d.txt", time.Now().Unix())
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

func BucketPresignHeadObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	content := "test content"
	buffer := bytes.NewBufferString(content)
	err := bucket.PutObject(context.Background(), objectKey, buffer)
	assert.Nil(t, err)
	defer bucket.RemoveObject(context.Background(), objectKey)

	presignBucket := bucket.(sdk.PresignBucket)
	signedURL, err := presignBucket.PresignHeadObject(context.Background(), objectKey, time.Second)
	assert.Nil(t, err)

	_, err = sdk.HeadObjectWithURL(signedURL, 5*time.Second)
	assert.Nil(t, err)
}

func BucketPresignGetObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	content := "test content"
	buffer := bytes.NewBufferString(content)
	err := bucket.PutObject(context.Background(), objectKey, buffer)
	assert.Nil(t, err)
	defer bucket.RemoveObject(context.Background(), objectKey)

	presignBucket := bucket.(sdk.PresignBucket)
	signedURL, err := presignBucket.PresignGetObject(context.Background(), objectKey, time.Second)
	assert.Nil(t, err)

	reader, err := sdk.GetObjectWithURL(signedURL, 5*time.Second)
	assert.Nil(t, err)
	bytes, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, content, string(bytes))
}

func BucketPresignPutObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	presignBucket := bucket.(sdk.PresignBucket)
	signedURL, err := presignBucket.PresignPutObject(context.Background(), objectKey, time.Second)
	assert.Nil(t, err)

	content := "test content"
	buffer := bytes.NewBufferString(content)
	err = sdk.PutObjectWithURL(signedURL, buffer, 5*time.Second)
	assert.Nil(t, err)
	defer bucket.RemoveObject(context.Background(), objectKey)

	reader, err := bucket.GetObject(context.Background(), objectKey)
	assert.Nil(t, err)
	bytes, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, content, string(bytes))
}
