package object_storage_sdk

// ACLType bucket/object ACL
type ACLType string

const (
	// ACLPrivate definition : private read and write
	ACLPrivate ACLType = "private"

	// ACLPublicRead definition : public read and private write
	ACLPublicRead ACLType = "public-read"

	// ACLPublicReadWrite definition : public read and public write
	ACLPublicReadWrite ACLType = "public-read-write"

	// ACLDefault Object. It's only applicable for object.
	ACLDefault ACLType = "default"
)

type Option func(config *config)

type config struct {
	ACLType ACLType
}

func GetConfig(options ...Option) *config {
	defaultConfig := &config{
		ACLType: ACLPublicRead,
	}

	for _, option := range options {
		option(defaultConfig)
	}

	return defaultConfig
}

func WithACL(aclType ACLType) Option {
	return func(config *config) {
		config.ACLType = aclType
	}
}
