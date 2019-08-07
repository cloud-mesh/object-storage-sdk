package huaweicloud_obs

import (
	sdk "github.com/inspii/object_storage_sdk"
	"github.com/inspii/object_storage_sdk/impl/huaweicloud_obs/obs"
)

var aclMap = map[obs.AclType]sdk.ACLType{
	obs.AclPrivate:         sdk.ACLPrivate,
	obs.AclPublicRead:      sdk.ACLPublicRead,
	obs.AclPublicReadWrite: sdk.ACLPublicReadWrite,
}

func obsAcl(acl sdk.ACLType) obs.AclType {
	for ossAcl, sdkAcl := range aclMap {
		if acl == sdkAcl {
			return ossAcl
		}
	}

	return ""
}

func sdkAcl(ossAcl obs.AclType) sdk.ACLType {
	sdkAcl, _ := aclMap[ossAcl]
	return sdkAcl
}
