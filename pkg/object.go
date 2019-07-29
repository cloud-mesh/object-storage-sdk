package pkg

import (
	"net/url"
	"time"
)

type policyCondition struct {
	matchType string
	condition string
	value     string
}

type PostPolicy struct {

	expiration time.Time

	conditions []policyCondition

	contentLengthRange struct {
		min int64
		max int64
	}

	formData map[string]string
}

type ObjectPresign interface {
	Presign(method string, bucketName string, objectName string, expires time.Duration, reqParams url.Values) (u *url.URL, err error)
	PresignedPostPolicy(p *PostPolicy) (u *url.URL, formData map[string]string, err error)
}
