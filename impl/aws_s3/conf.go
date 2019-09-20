package aws_s3

import (
	sdk "github.com/inspii/object-storage-sdk"
)

const (
	aclPrivate         string = "private"
	aclPublicRead      string = "public-read"
	aclPublicReadWrite string = "public-read-write"
)

var aclMap = map[string]sdk.ACLType{
	aclPrivate:         sdk.ACLPrivate,
	aclPublicRead:      sdk.ACLPublicRead,
	aclPublicReadWrite: sdk.ACLPublicReadWrite,
}

func awsAcl(acl sdk.ACLType) string {
	for ossAcl, sdkAcl := range aclMap {
		if acl == sdkAcl {
			return ossAcl
		}
	}

	return ""
}

func sdkAcl(ossAcl string) sdk.ACLType {
	sdkAcl, _ := aclMap[ossAcl]
	return sdkAcl
}
