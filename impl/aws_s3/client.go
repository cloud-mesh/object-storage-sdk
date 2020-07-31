package aws_s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	sdk "github.com/cloud-mesh/object-storage-sdk"
)

func NewClient(region, endpoint string, accessKeyID, accessKeySecret string, config *aws.Config) (*s3Client, error) {
	if config == nil {
		config = new(aws.Config)
	}
	config.Region = aws.String(region)
	config.Endpoint = aws.String(endpoint)
	config.Credentials = credentials.NewStaticCredentials(accessKeyID, accessKeySecret, "")

	sess, err := session.NewSessionWithOptions(session.Options{Config: *config})
	if err != nil {
		return nil, err
	}
	client := s3.New(sess)

	return &s3Client{
		config:  config,
		session: sess,
		client:  client,
	}, nil
}

type s3Client struct {
	config  *aws.Config
	session *session.Session
	client  *s3.S3
}

func (c *s3Client) Bucket(bucketName string) (bucket sdk.BasicBucket, err error) {
	return newS3Bucket(bucketName, c), nil
}

func (c *s3Client) MakeBucket(bucketName string, options ...sdk.Option) error {
	config := sdk.GetConfig(options...)
	input := &s3.CreateBucketInput{
		ACL:    aws.String(awsAcl(config.ACLType)),
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: c.config.Region,
		},
	}
	_, err := c.client.CreateBucket(input)
	return err
}

func (c *s3Client) HeadBucket(bucketName string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err := c.client.HeadBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return sdk.BucketNotExist
			}
		}
	}

	return err
}

func (c *s3Client) GetBucketLocation(bucketName string) (location string, err error) {
	input := &s3.GetBucketLocationInput{
		Bucket: aws.String(bucketName),
	}

	output, err := c.client.GetBucketLocation(input)
	if err != nil {
		return
	}

	return output.String(), nil
}

func (c *s3Client) ListBucket(options ...sdk.Option) (buckets []sdk.BucketProperties, err error) {
	input := &s3.ListBucketsInput{}
	output, err := c.client.ListBuckets(input)
	if err != nil {
		return
	}

	for _, bucket := range output.Buckets {
		buckets = append(buckets, sdk.BucketProperties{
			Name:      aws.StringValue(bucket.Name),
			CreatedAt: aws.TimeValue(bucket.CreationDate),
		})
	}
	return
}

func (c *s3Client) RemoveBucket(bucketName string) error {
	input := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err := c.client.DeleteBucket(input)
	return err
}

func (c *s3Client) CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey string) error {
	copySource := fmt.Sprintf("%s/%s", srcBucketName, srcObjectKey)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucketName),
		Key:        aws.String(dstObjectKey),
		CopySource: &copySource,
	}
	_, err := c.client.CopyObject(input)
	return err
}
