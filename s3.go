package goucsdnt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
)

const UCSDNT_S3_ENDPOINT = "https://hermes.caida.org/"

const UCSDNT_S3_PCAPLIVE = "telescope-ucsdnt-pcap-live"
const UCSDNT_S3_FT = "telescope-ucsdnt-avro-flowtuple-v4-2024"

type UCSDNTBucket struct {
	S3Client *s3.Client
	Ctx      context.Context
}

type staticResolver struct{}

func (*staticResolver) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (
	smithyendpoints.Endpoint, error,
) {
	// This value will be used as-is when making the request.
	/*	if len(*params.Endpoint) == 0 {
		u, err := url.Parse(UCSDNT_S3_ENDPOINT)
		if err != nil {
			return smithyendpoints.Endpoint{}, err
		}
		return smithyendpoints.Endpoint{
			URI: *u,
		}, nil
	}*/
	// s3.Options.BaseEndpoint is accessible here:
	fmt.Printf("The endpoint provided in config is %s\n", *params.Endpoint)

	//default
	return s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
}

func NewUCSDNTBucket(ctx context.Context) *UCSDNTBucket {
	//read the key and secret from the environment
	UCSD_NT_S3_ACCESS_KEY := os.Getenv("UCSD_NT_S3_ACCESS_KEY")
	UCSD_NT_S3_SECRET_KEY := os.Getenv("UCSD_NT_S3_SECRET_KEY")
	fmt.Println("Access key:", UCSD_NT_S3_ACCESS_KEY)
	fmt.Println("Secret key:", UCSD_NT_S3_SECRET_KEY)
	/*tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	hclient := &http.Client{Transport: tr}*/
	client := s3.New(s3.Options{
		BaseEndpoint:       aws.String(UCSDNT_S3_ENDPOINT),
		EndpointResolverV2: &staticResolver{},
		Credentials:        aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(UCSD_NT_S3_ACCESS_KEY, UCSD_NT_S3_SECRET_KEY, "")),
		Region:             "auto",
		UsePathStyle:       true,
		//ClientLogMode:      aws.LogRetries | aws.LogRequest | aws.LogResponse,
		//HTTPClient:         hclient,
	})
	fmt.Println("Client created", client)
	return &UCSDNTBucket{
		S3Client: client,
		Ctx:      ctx,
	}
}

func (b *UCSDNTBucket) ListObjects() ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(b.S3Client, &s3.ListObjectsV2Input{
		//Bucket: aws.String(UCSDNT_S3_PCAPLIVE),
		Bucket: aws.String(UCSDNT_S3_FT),
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

func (b *UCSDNTBucket) GetObjectByDatetime(d time.Time) (io.Reader, error) {
	objpath := d.Format("datasource=ucsd-nt/year=2006/month=01/day=02/hour=15/")
	pcapname := fmt.Sprintf("ucsd-nt.%d.pcap.gz", d.Unix())

	input := &s3.GetObjectInput{
		Bucket: aws.String(UCSDNT_S3_PCAPLIVE),
		Key:    aws.String(filepath.Join(objpath, pcapname)),
	}
	result, err := b.S3Client.GetObject(b.Ctx, input)

	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s. No such key exists.\n", *input.Bucket)
			err = noKey
		} else {
			log.Printf("Couldn't get object %v. Here's why: %v\n", *input.Bucket, err)
		}
		return nil, err
	}
	return result.Body, nil
}
