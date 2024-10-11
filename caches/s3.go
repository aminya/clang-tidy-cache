package caches

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type S3Configuration struct {
	BaseEndpoint string `json:"baseEndpoint"`
	BucketName   string `json:"bucketName"`
}

type S3Cache struct {
	ctx        context.Context
	client     *s3.Client
	bucketName string
}

func getS3BaseEndpoint() (string, error) {
	addr := os.Getenv("CLANG_TIDY_CACHE_S3_BASE_ENDPOINT")
	if addr == "" {
		return "", errors.New("`CLANG_TIDY_CACHE_S3_BASE_ENDPOINT` must be set")
	}

	return addr, nil
}

func getS3BucketName() string {
	return os.Getenv("CLANG_TIDY_CACHE_S3_BUCKET_NAME")
}

func NewS3Cache(cfg *S3Configuration) (*S3Cache, error) {
	var baseEndpoint string
	if cfg.BaseEndpoint == "" {
		var err error
		baseEndpoint, err = getS3BaseEndpoint()

		if err != nil {
			return nil, err
		}
	} else {
		baseEndpoint = cfg.BaseEndpoint
	}

	ctx := context.TODO()

	// Load the Shared AWS Configuration (~/.aws/config)
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
	})

	var bucketName string
	if cfg.BucketName == "" {
		bucketName = getS3BucketName()
	} else {
		bucketName = cfg.BucketName
	}

	cache := S3Cache{
		ctx:        ctx,
		client:     client,
		bucketName: bucketName,
	}

	bucketExist, err := cache.BucketExists()
	if err != nil {
		log.Fatal(err)
	}
	if !bucketExist {
		log.Fatalf("Bucket %v not exist", bucketName)
	}

	return &cache, nil
}

func (c *S3Cache) BucketExists() (bool, error) {
	_, err := c.client.HeadBucket(c.ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucketName),
	})
	exists := true
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				log.Printf("Bucket %v is available.\n", c.bucketName)
				exists = false
				err = nil
			default:
				log.Printf("Either you don't have access to bucket %v or another error occurred. "+
					"Here's what happened: %v\n", c.bucketName, err)
			}
		}
	} else {
		log.Printf("Bucket %v exists and you already own it.", c.bucketName)
	}
	return exists, err
}

func (c *S3Cache) FindEntry(digest []byte) ([]byte, error) {
	objectKey := hex.EncodeToString(digest)

	result, err := c.client.GetObject(c.ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v. Here's why: %v\n", c.bucketName, objectKey, err)
		return nil, err
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v. Here's why: %v\n", objectKey, err)
	}
	return body, err
}

func (c *S3Cache) SaveEntry(digest []byte, content []byte) error {
	objectKey := hex.EncodeToString(digest)
	buffer := bytes.NewReader(content)

	_, err := c.client.PutObject(c.ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(objectKey),
		Body:   buffer,
	})
	if err != nil {
		log.Printf("Couldn't upload object to %v:%v. Here's why: %v\n",
			c.bucketName, objectKey, err)
	}
	return err
}
