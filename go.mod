module github.com/shivanihariprasad/goucsdnt

go 1.21.1

require (
	github.com/aws/aws-sdk-go-v2 v1.34.0
	github.com/aws/aws-sdk-go-v2/credentials v1.17.55
	github.com/aws/aws-sdk-go-v2/service/s3 v1.74.1
	github.com/aws/smithy-go v1.22.2
	github.com/google/gopacket v1.1.19
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.10 // indirect
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859 // indirect
	golang.org/x/sys v0.0.0-20190412213103-97732733099d // indirect
)

//replace github.com/shivanihariprasad/goucsdnt => ../goucsdnt
