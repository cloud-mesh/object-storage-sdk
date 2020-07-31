package aliyun_oss

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	sdk "github.com/cloud-mesh/object-storage-sdk"
)

var aclMap = map[oss.ACLType]sdk.ACLType{
	oss.ACLPrivate:         sdk.ACLPrivate,
	oss.ACLPublicRead:      sdk.ACLPublicRead,
	oss.ACLPublicReadWrite: sdk.ACLPublicReadWrite,
}

func ossAcl(acl sdk.ACLType) oss.ACLType {
	for ossAcl, sdkAcl := range aclMap {
		if acl == sdkAcl {
			return ossAcl
		}
	}

	return ""
}

func sdkAcl(ossAcl oss.ACLType) sdk.ACLType {
	sdkAcl, _ := aclMap[ossAcl]
	return sdkAcl
}
