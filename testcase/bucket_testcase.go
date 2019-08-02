package testcase

import (
	"fmt"
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

func BucketObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	objectKey := fmt.Sprintf("testobjecta%d.txt", time.Now().Unix())
	buffer := strings.NewReader("test content")
	err := bucket.PutObject(objectKey, buffer)
	assert.Nil(t, err)

	_, err = bucket.HeadObject(objectKey)
	assert.Nil(t, err)

	_, err = bucket.GetObject(objectKey)
	assert.Nil(t, err)

	err = bucket.RemoveObject(objectKey)
	assert.Nil(t, err)
}

func BucketObjectsTest(t *testing.T, bucket sdk.BasicBucket) {
	buffer := strings.NewReader("test content")
	objectKeyA := fmt.Sprintf("testobjecta%d.txt", time.Now().Unix())
	objectKeyB := fmt.Sprintf("testobjectb%d.txt", time.Now().Unix())
	objectKeyC := fmt.Sprintf("testobjectc%d.txt", time.Now().Unix())
	err := bucket.PutObject(objectKeyA, buffer)
	assert.Nil(t, err)
	err = bucket.PutObject(objectKeyB, buffer)
	assert.Nil(t, err)
	err = bucket.CopyObject(objectKeyA, objectKeyC)
	assert.Nil(t, err)

	objects, err := bucket.ListObjects("testobject")
	assert.Nil(t, err)
	assert.Len(t, objects, 3)

	err = bucket.RemoveObjects([]string{objectKeyA, objectKeyB, objectKeyC})
	assert.Nil(t, err)

	objects, err = bucket.ListObjects("testobject")
	assert.Nil(t, err)
	assert.Len(t, objects, 0)
}

func BucketPresignHeadObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	presignBucket, ok := bucket.(sdk.PresignAbleBucket)
	if !ok {
		t.Skip()
	}

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	content := "test content"
	buffer := strings.NewReader(content)
	err := bucket.PutObject(objectKey, buffer)
	assert.Nil(t, err)
	defer bucket.RemoveObject(objectKey)

	signedURL, err := presignBucket.PresignHeadObject(objectKey, time.Second)
	assert.Nil(t, err)

	_, err = sdk.HeadObjectWithURL(signedURL, 5*time.Second)
	assert.Nil(t, err)
}

func BucketPresignGetObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	presignBucket, ok := bucket.(sdk.PresignAbleBucket)
	if !ok {
		t.Skip()
	}

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	content := "test content"
	buffer := strings.NewReader(content)
	err := bucket.PutObject(objectKey, buffer)
	assert.Nil(t, err)
	defer bucket.RemoveObject(objectKey)

	signedURL, err := presignBucket.PresignGetObject(objectKey, time.Second)
	assert.Nil(t, err)

	reader, err := sdk.GetObjectWithURL(signedURL, 5*time.Second)
	assert.Nil(t, err)
	bytes, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, content, string(bytes))
}

func BucketPresignPutObjectTest(t *testing.T, bucket sdk.BasicBucket) {
	presignBucket, ok := bucket.(sdk.PresignAbleBucket)
	if !ok {
		t.Skip()
	}

	objectKey := fmt.Sprintf("testobject%d.txt", time.Now().Unix())
	signedURL, err := presignBucket.PresignPutObject(objectKey, time.Second)
	assert.Nil(t, err)

	content := "test content"
	buffer := strings.NewReader(content)
	err = sdk.PutObjectWithURL(signedURL, buffer, 5*time.Second)
	assert.Nil(t, err)
	defer bucket.RemoveObject(objectKey)

	reader, err := bucket.GetObject(objectKey)
	assert.Nil(t, err)
	bytes, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, content, string(bytes))
}
