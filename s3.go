package goucsdnt

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const UCSDNT_S3_ENDPOINT = "https://hermes.caida.org"
const UCSDNT_S3_PCAPLIVE = "telescope-ucsdnt-pcap-live"

type UCSDNTBucket struct {
	S3Client *s3.Client
	Ctx      context.Context
}

func NewUCSDNTBucket(ctx context.Context) *UCSDNTBucket {
	//read the key and secret from the environment
	UCSD_NT_S3_ACCESS_KEY := os.Getenv("UCSD_NT_S3_ACCESS_KEY")
	UCSD_NT_S3_SECRET_KEY := os.Getenv("UCSD_NT_S3_SECRET_KEY")
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(UCSDNT_S3_ENDPOINT),
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(UCSD_NT_S3_ACCESS_KEY, UCSD_NT_S3_SECRET_KEY, "")),
	})
	return &UCSDNTBucket{
		S3Client: client,
		Ctx:      ctx,
	}
}

func (b *UCSDNTBucket) ListObjects() ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(b.S3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(UCSDNT_S3_PCAPLIVE),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(b.Ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}
