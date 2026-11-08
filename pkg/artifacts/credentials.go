package artifacts

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// resolveCredentialsTimeout bounds asking the default credential chain at boot.
// The chain may ask a metadata service or a token service over the network, and
// a boot that hangs on one says nothing.
const resolveCredentialsTimeout = 30 * time.Second

// CheckS3Keys refuses a static key that is not whole. The two settings are one
// credential: half of one is a mistake, and using the default credential chain
// beside it would sign as someone the operator did not name. No error here
// carries a value.
func (c Config) CheckS3Keys() error {
	switch {
	case c.S3AccessKey != "" && c.S3SecretKey == "":
		return errors.New("artifacts: object store credentials: ORK_S3_ACCESS_KEY is set and ORK_S3_SECRET_KEY is not: " +
			"set both, or neither to use the default credential chain")
	case c.S3AccessKey == "" && c.S3SecretKey != "":
		return errors.New("artifacts: object store credentials: ORK_S3_SECRET_KEY is set and ORK_S3_ACCESS_KEY is not: " +
			"set both, or neither to use the default credential chain")
	case c.S3AccessKey == "" && c.S3SessionToken != "":
		return errors.New("artifacts: object store credentials: ORK_S3_SESSION_TOKEN is set and neither " +
			"ORK_S3_ACCESS_KEY nor ORK_S3_SECRET_KEY is: a session token belongs to a key, so set all three, " +
			"or none to use the default credential chain")
	}
	return nil
}

// ResolveS3Credentials asks for the credential cfg's S3 stores will sign with,
// and returns its source: "StaticCredentials" for the configured key, otherwise
// the name the default credential chain gives the member that answered. A boot
// calls it so that a deployment with nothing to sign with stops there, rather
// than a run failing later. The source and the error never carry a credential.
func ResolveS3Credentials(ctx context.Context, cfg Config) (string, error) {
	if err := cfg.CheckS3Keys(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, resolveCredentialsTimeout)
	defer cancel()
	awsCfg, err := loadS3Config(ctx, cfg)
	if err != nil {
		return "", err
	}
	creds, err := awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return "", fmt.Errorf("artifacts: object store credentials: ORK_S3_ACCESS_KEY and ORK_S3_SECRET_KEY are unset "+
			"and the default credential chain found none (environment, shared files, web identity, container or "+
			"instance role): %w", err)
	}
	return creds.Source, nil
}
