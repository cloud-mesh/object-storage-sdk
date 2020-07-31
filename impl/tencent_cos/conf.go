package tencent_cos

import (
	"context"
	sdk "github.com/cloud-mesh/object-storage-sdk"
	"time"
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

func cosAcl(acl sdk.ACLType) string {
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

type config struct {
	timeout time.Duration
	useSSL  bool
}

func (c *config) NewContext() (context.Context, context.CancelFunc) {
	if c.timeout > 0 {
		return context.WithTimeout(context.Background(), c.timeout)
	}

	return context.Background(), func() {}
}

type option func(*config) ()

func WithTimeOut(timeout time.Duration) option {
	return func(config *config) {
		config.timeout = timeout
	}
}

func WithSSL() option {
	return func(config *config) {
		config.useSSL = true
	}
}
